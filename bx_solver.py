"""
bx-solver v3 — self-hosted Turnstile minting service (patchright + patched chromium).

Why this exists (R&D conclusions of 2026-11-15..17, see WORKLOG.md):
  * patchright stealth was PROVEN: a neutral Turnstile demo mints in ~1.2s with
    ZERO clicks in headless AND headful mode.
  * Wall #1 (OBSOLETE 2026-11-17): brunhild.challenges.cloudflare.com is gone
    from DNS entirely (A and AAAA both empty) and the real widget never
    contacts it — everything runs on challenges.cloudflare.com/cdn-cgi/
    challenge-platform (dual-stack, v4-reachable). BX_BRUNHILD_PIN is kept only
    for backward compat and does nothing; leave it unset.
  * Wall #2 (measured 2026-11-17, the honest physics): PRODUCTION sitekeys
    refuse self-hosted mints —
      - unbound challenges (no per-task cdata): the /h/b/fo/ fingerprint
        submission answers HTTP 400 and the challenge is dead on arrival;
      - cdata-bound challenges (LootLabs): the challenge RUNS (fo ok,
        interactive checkbox, click lands) but completion never arrives —
        /h/b/pat/ answers 401 (Private Access Token refused) and the widget
        spins until the budget dies. Proven INVARIANT across chromium/real-
        Chrome, headless/headful (Xvfb), and two datacenter egress IPs;
        scrape.do proxy-mode egress breaks the challenge flow entirely.
        Conclusion: the completion check demands a client class a Linux
        datacenter box cannot satisfy (provider fleets can).
  * What this service is FOR, therefore: (a) neutral/dummy sitekeys mint free
    forever; (b) for production sitekeys it is the FAST HONEST FIRST LEG —
    v3 classifies the failure and doom-caches it, so a doomed sitekey costs
    the lane ~1.5s instead of a full 45s budget; (c) real-page /walk for
    lenient sitekeys.

Mint modes:
  POST /solve  — generic mint: renders a MINIMAL local widget page at the REAL
                 origin of `url` (the document request is fulfilled locally, the
                 browser URL still shows the real site) with the EXACT
                 sitekey/action/cdata binding. Fast path (~1.5-3s warm).
  POST /walk   — real-page walk: loads the actual captcha URL and lets the
                 page's OWN widget mint (binding + hostname honored by
                 construction). Optional auto-click.
  GET  /health — liveness + live metrics (p50/p95, in-flight, per-sitekey table).
  POST /warm   — prelaunch the browser (the watchdog hits /health; startup
                 warms automatically when BX_WARMUP=1).

Auth: when BX_SOLVER_TOKEN is set every request must send
      "Authorization: Bearer <token>" (401 otherwise).

v3 over v2 (2026-11-17):
  * FAILURE CLASSIFICATION: every failed mint gets a machine-readable
    failClass — "fo-reject" (4xx on the fo submission: dead challenge,
    detected EARLY — the loop aborts in seconds instead of burning budget),
    "interactive-stall" (challenge ran, click landed, completion never came),
    "widget-error", "shape", "driver", "transient". Response carries
    {failClass, advice}; the lane can skip ahead instantly.
  * PER-SITEKEY DOOM CACHE: after DOOM_AFTER (default 2) consecutive
    deterministic failures for a sitekey, mints for it answer in <1.5s with
    failClass="doom-cached" until DOOM_TTL_S (default 1800) elapses — then
    ONE full-budget re-probe keeps the system adaptive to Cloudflare
    relaxing the wall.
  * WARM CONTEXT POOL: POOL_SIZE (default = max_concurrent) contexts are
    pre-created in the background; a mint borrows one instead of paying
    ~0.5-1s of context setup. The pool refills itself after each borrow.
  * LIVE METRICS: /health now carries p50_ms/p95_ms (successful mints window),
    inflight, queued, per-sitekey table {ok, fail, classes, fastfails,
    last_ms, p50_ms, p95_ms}, browser age, context pool depth.
  * RETRY HARDENING: driver-class failures (browser/context died mid-mint,
    e.g. during a background recycle) retry on a fresh context within the
    remaining budget; transient dry rounds keep the 65/35 budget split;
    classified deterministic failures never retry (that was v2 and stays).
  * OPTIONAL PROXY PASSTHROUGH: BX_PROXY="http://host:port|user|pass" env or
    per-request {"proxy": {...}} — routes the mint context through an HTTP
    proxy. EXPERIMENTAL: measured 2026-11-17 to NOT unlock the LootLabs wall
    (the challenge assets misbehave through the MITM tunnel); kept for
    lenient-sitekey scenarios where egress reputation matters.

Env:
  PORT               Render injects this — takes precedence when set
  BX_SOLVER_PORT     default 8787
  BX_SOLVER_TOKEN    shared secret (optional but recommended on a public VPS)
  BX_HOST            bind address, default 0.0.0.0
  BX_MAX_CONCURRENT  parallel mints, default 2 (each context ~60-120MB)
  BX_HEADLESS        1 (default) | 0 (headful — needs Xvfb on servers)
  BX_BRUNHILD_PIN    0 (default) | 1 — OBSOLETE: brunhild is gone from DNS and
                     the widget never contacts it; kept for compat, does nothing
  BX_WARMUP          1 (default) | 0 — launch the browser at boot
  BX_QUEUE_WAIT_S    bounded semaphore wait before an honest 429 (default 12)
  BX_RETRY           1 (default) | 0 — one in-budget retry on transient dry
                     rounds AND driver failures (never on deterministic ones)
  BX_BROWSER_RECYCLE_MINTS  browser relaunch cadence, default 40 (0 = off)
  BX_RSS_ABORT_MB    mint guard: abort the mint (honest fail) if the whole
                     tree exceeds this MB — default 4600 (0 = off)
  BX_POOL_SIZE       warm context pool depth, default = max_concurrent
  BX_DOOM_AFTER      consecutive deterministic fails before doom-caching a
                     sitekey, default 2 (0 = off)
  BX_DOOM_TTL_S      doom cache lifetime, default 1800 (one full re-probe per
                     TTL keeps it adaptive)
  BX_PROXY           optional default proxy "server|user|pass" for mint contexts
  BX_RENDER_FREE     0 (default) | 1 — Render free-tier profile (512MB RAM /
                     0.1 CPU): memory-trimmed chromium, concurrency forced to
                     1, 640x480 viewport, browser torn down after
                     BX_BROWSER_IDLE_KILL_S idle (default 75)
Run:  python bx_solver.py   (or the systemd unit, see install.sh / README.md)
"""

import asyncio
import gc
import json
import os
import re
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

# Render web services must bind $PORT; on a VPS BX_SOLVER_PORT (8787) wins.
PORT = int(os.environ.get("PORT") or os.environ.get("BX_SOLVER_PORT", "8787"))
HOST = os.environ.get("BX_HOST", "0.0.0.0")
AUTH_TOKEN = os.environ.get("BX_SOLVER_TOKEN", "").strip()
MAX_CONCURRENT = max(1, int(os.environ.get("BX_MAX_CONCURRENT", "2")))
HEADLESS = os.environ.get("BX_HEADLESS", "1") != "0"
BRUNHILD_PIN = os.environ.get("BX_BRUNHILD_PIN", "0") == "1"

RENDER_FREE = os.environ.get("BX_RENDER_FREE", "0") == "1"
IDLE_KILL_S = int(os.environ.get("BX_BROWSER_IDLE_KILL_S", "75" if RENDER_FREE else "0"))
if RENDER_FREE:
    MAX_CONCURRENT = 1  # 0.1 CPU + 512MB: strictly one mint at a time

WARMUP = (os.environ.get("BX_WARMUP", "1" if not RENDER_FREE else "0") == "1")
QUEUE_WAIT_S = max(1, int(os.environ.get("BX_QUEUE_WAIT_S", "12")))
RETRY_ENABLED = os.environ.get("BX_RETRY", "1") == "1"
RECYCLE_MINTS = 0 if RENDER_FREE else max(0, int(os.environ.get("BX_BROWSER_RECYCLE_MINTS", "40")))
RSS_ABORT_MB = int(os.environ.get("BX_RSS_ABORT_MB", "460" if RENDER_FREE else "4600"))
POOL_SIZE = max(1, int(os.environ.get("BX_POOL_SIZE", str(MAX_CONCURRENT))))
DOOM_AFTER = max(0, int(os.environ.get("BX_DOOM_AFTER", "2")))
DOOM_TTL_S = max(60, int(os.environ.get("BX_DOOM_TTL_S", "1800")))
_PROXY_RAW = os.environ.get("BX_PROXY", "").strip()
DEFAULT_PROXY = (
    dict(zip(("server", "username", "password"), (_PROXY_RAW.split("|") + ["", ""])[:3]))
    if _PROXY_RAW and "|" in _PROXY_RAW
    else None
)

# minted tokens observed in the wild: versioned "1."-prefixed (816 chars seen
# live) and the legacy "0."-prefixed shape — same validation the lane uses.
TOKEN_RE = re.compile(r"^\d\.[A-Za-z0-9_.-]{80,}$")
TOKEN_FALLBACK_RE = re.compile(r"0\.[A-Za-z0-9_-]{100,}")

SOLVE_BUDGET_CAP_MS = 90_000
WALK_BUDGET_CAP_MS = 120_000
CLICK_AT_FRACTIONS = (0.33, 0.62)  # of the budget; proven cadence from the R&D probe
RETRY_SPLIT = 0.65  # attempt 1 gets this share of the budget; the retry gets the rest

VERSION = "3.0.0"
_START = time.time()

_ms_window: deque[int] = deque(maxlen=20)
_stats = {
    "solves_ok": 0, "solves_fail": 0, "walks_ok": 0, "walks_fail": 0,
    "retries": 0, "busy_429": 0, "recycles": 0, "fastfails": 0,
    "last_ms": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0,
    "peak_rss_mb": 0, "last_diag": "", "last_fail_class": "",
    "last_error": "", "version": VERSION,
}
_sitekeys: dict[str, dict] = {}
_doom: dict[str, dict] = {}  # sitekey -> {"until": epoch, "failClass": str}
_recent_diags: deque[dict] = deque(maxlen=20)

ADVICE = {
    "fo-reject": "challenge rejected at fingerprint submission — sitekey/binding or client class refused; do NOT retry, fall through to the next provider",
    "interactive-stall": "challenge ran and the click landed but completion never arrived (client-class wall, e.g. PAT refusal) — fall through to a provider-class solver",
    "widget-error": "widget error-callback fired — deterministic, fall through",
    "shape": "minted token failed the shape gate — deterministic, fall through",
    "driver": "browser/context crashed mid-mint — retry may help",
    "transient": "dry round (nothing minted before deadline) — a redraw can mint",
    "doom-cached": "sitekey doom-cached after repeated deterministic failures — skip this solver until the TTL re-probe",
}


def _log(line: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {line}", flush=True)


def _tree_rss_mb() -> int:
    """Own + all descendant footprint in MB, measured as PSS (proportional
    share of shared pages — the honest cgroup-style number) with VmRSS as
    fallback. Chromium runs as our child tree, so this is the true footprint
    gauge for a memory-capped host."""
    try:
        me = os.getpid()
        ppid_of: dict[int, int] = {}
        rss_of: dict[int, int] = {}
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            pid = int(entry)
            try:
                with open(f"/proc/{entry}/stat", "rb") as f:
                    raw = f.read().decode("utf-8", "replace")
                rest = raw[raw.rindex(")") + 1:].split()
                ppid_of[pid] = int(rest[1])
                val = 0
                try:
                    with open(f"/proc/{entry}/smaps_rollup") as f:
                        for line in f:
                            if line.startswith("Pss:"):
                                val = int(line.split()[1])  # kB
                                break
                except Exception:
                    val = 0
                if not val:
                    with open(f"/proc/{entry}/status") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                val = int(line.split()[1])  # kB
                                break
                rss_of[pid] = val
            except Exception:
                continue
        total = rss_of.get(me, 0)
        frontier, seen = [me], {me}
        while frontier:
            cur = frontier.pop()
            for pid, pp in ppid_of.items():
                if pp == cur and pid not in seen:
                    seen.add(pid)
                    total += rss_of.get(pid, 0)
                    frontier.append(pid)
        return total // 1024
    except Exception:
        return 0


def _note_rss() -> None:
    mb = _tree_rss_mb()
    if mb > _stats["peak_rss_mb"]:
        _stats["peak_rss_mb"] = mb


_rss_last_check = 0.0


def _rss_guard_diag() -> str | None:
    """Mint guard, throttled to ~1 check/second. Aborting the mint honestly is
    strictly better than letting the cgroup OOM-kill the service."""
    global _rss_last_check
    if not RSS_ABORT_MB:
        return None
    now = time.time()
    if now - _rss_last_check < 1.0:
        return None
    _rss_last_check = now
    mb = _tree_rss_mb()
    if mb > RSS_ABORT_MB:
        return f"rss-guard: tree hit {mb}MB (abort at {RSS_ABORT_MB}MB)"
    return None


def _pctl(window, q):
    if not window:
        return 0
    s = sorted(window)
    idx = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[idx]


def _sitekey_rec(sk: str) -> dict:
    rec = _sitekeys.get(sk)
    if rec is None:
        rec = {
            "ok": 0, "fail": 0, "fastfails": 0, "classes": {},
            "last_ms": 0, "p50_ms": 0, "p95_ms": 0, "_window": deque(maxlen=20),
            "doom_streak": 0,
        }
        _sitekeys[sk] = rec
    return rec


def _classify_and_remember(sk: str, fail_class: str, ok: bool, ms: int) -> None:
    rec = _sitekey_rec(sk)
    if ok:
        rec["ok"] += 1
        rec["_window"].append(ms)
        rec["p50_ms"] = _pctl(rec["_window"], 0.5)
        rec["p95_ms"] = _pctl(rec["_window"], 0.95)
        rec["last_ms"] = ms
        rec["doom_streak"] = 0
        _doom.pop(sk, None)
        return
    rec["fail"] += 1
    rec["classes"][fail_class] = rec["classes"].get(fail_class, 0) + 1
    if fail_class in ("fo-reject", "interactive-stall"):
        rec["doom_streak"] += 1
        if DOOM_AFTER and rec["doom_streak"] >= DOOM_AFTER:
            _doom[sk] = {"until": time.time() + DOOM_TTL_S, "failClass": fail_class}
            _log(f"doom-cache: {sk[:22]}… {fail_class} x{rec['doom_streak']} — TTL {DOOM_TTL_S}s")
    else:
        rec["doom_streak"] = 0


def _doom_check(sk: str) -> dict | None:
    d = _doom.get(sk)
    if not d:
        return None
    if time.time() >= d["until"]:
        # TTL expired — allow ONE full-budget re-probe (adaptive to CF changes)
        _doom.pop(sk, None)
        _sitekey_rec(sk)["doom_streak"] = 0
        _log(f"doom-cache: TTL expired for {sk[:22]}… — full re-probe")
        return None
    return d


# ---------------------------------------------------------------- browser pool
class BrowserPool:
    """Patched chromium lifecycle.
    Standard profile: launched warm at boot (BX_WARMUP=1), kept up, recycled
    every BX_BROWSER_RECYCLE_MINTS mints (background relaunch — zero latency
    cost), mint borrows a PRE-WARMED CONTEXT from the pool.
    Render-free profile: browser reused only within the idle window (burst
    warmth), torn down afterwards — idle footprint returns to ~100MB."""

    def __init__(self) -> None:
        self._pw = None
        self._browser = None
        self._idle_task: asyncio.Task | None = None
        self._warm_task: asyncio.Task | None = None
        self._ctx_pool: asyncio.Queue = asyncio.Queue(maxsize=POOL_SIZE)
        self._ctx_refill: asyncio.Task | None = None
        self.lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(MAX_CONCURRENT)
        self.mints_since_launch = 0
        self.warming = False
        self.launched_at = 0.0

    def _launch_args(self) -> list:
        args = ["--disable-dev-shm-usage", "--no-sandbox"]
        if BRUNHILD_PIN:
            # OBSOLETE: brunhild.challenges.cloudflare.com is gone from DNS and
            # the widget never contacts it (2026-11-17 forensics). Kept only so
            # old env files don't break; the MAP is harmless.
            args.append(
                "--host-resolver-rules="
                "MAP brunhild.challenges.cloudflare.com 104.18.17.146"
            )
        if RENDER_FREE:
            args += [
                "--js-flags=--max-old-space-size=128",
                "--renderer-process-limit=1",
                "--disable-gpu",
                "--disable-features=BackForwardCache,Translate",
            ]
        return args

    async def browser(self):
        async with self.lock:
            return await self._ensure_browser_locked()

    async def _ensure_browser_locked(self):
        if self._idle_task is not None:
            self._idle_task.cancel()
            self._idle_task = None
        if self._browser is not None and self._browser.is_connected():
            return self._browser
        from patchright.async_api import async_playwright

        if self._pw is None:
            self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=HEADLESS,
            channel="chromium",  # patchright's patched build
            args=self._launch_args(),
        )
        self.mints_since_launch = 0
        self.launched_at = time.time()
        self._refill_pool()
        return self._browser

    async def _new_context(self, proxy_override: dict | None = None):
        browser = self._browser
        if browser is None or not browser.is_connected():
            browser = await self._ensure_browser_locked()
        vw, vh = (640, 480) if RENDER_FREE else (1280, 720)
        kwargs = {
            "locale": "en-US",
            "viewport": {"width": vw, "height": vh},
            "ignore_https_errors": True,
        }
        if proxy_override:
            kwargs["proxy"] = dict(proxy_override)
        elif DEFAULT_PROXY:
            kwargs["proxy"] = dict(DEFAULT_PROXY)
        return await browser.new_context(**kwargs)

    def _refill_pool(self) -> None:
        if self._ctx_refill is None or self._ctx_refill.done():
            self._ctx_refill = asyncio.create_task(self._pool_worker())

    async def _pool_worker(self):
        while True:
            if self._browser is None or not self._browser.is_connected():
                await asyncio.sleep(1.0)
                continue
            try:
                if self._ctx_pool.qsize() >= POOL_SIZE:
                    await asyncio.sleep(1.0)
                    continue
                ctx = await self._new_context()
                try:
                    self._ctx_pool.put_nowait(ctx)
                except asyncio.QueueFull:
                    await ctx.close()
            except Exception as e:
                _log(f"ctx-pool: refill error {type(e).__name__}: {str(e)[:90]}")
                await asyncio.sleep(2.0)

    async def borrow_context(self):
        """Take a pre-warmed context if one is ready (zero setup cost);
        otherwise create one on the spot (pool keeps filling in background)."""
        try:
            ctx = self._ctx_pool.get_nowait()
            return ctx
        except asyncio.QueueEmpty:
            return await self._new_context()

    def return_context(self, ctx) -> None:
        """Mint contexts are single-use (fingerprint hygiene): close them;
        the pool worker refills in the background."""
        async def closer():
            try:
                await ctx.close()
            except Exception:
                pass
        asyncio.create_task(closer())

    async def warm(self) -> dict:
        """Idempotent prelaunch; safe to call from startup, /warm and tests."""
        async with self.lock:
            if self._browser is not None and self._browser.is_connected():
                self._refill_pool()
                return {"ok": True, "browser": "up", "warming": False, "pool": self._ctx_pool.qsize()}
            self.warming = True
            try:
                await self._ensure_browser_locked()
                return {"ok": True, "browser": "up", "warming": False, "pool": self._ctx_pool.qsize()}
            except Exception as e:
                return {"ok": False, "error": str(e)[:160], "warming": False}
            finally:
                self.warming = False

    def warm_in_background(self) -> None:
        if self._warm_task is None or self._warm_task.done():
            self._warm_task = asyncio.create_task(self._warm_bg())

    async def _warm_bg(self):
        t0 = time.time()
        r = await self.warm()
        _log(f"warmup browser={r.get('browser')} ms={int((time.time()-t0)*1000)} err={r.get('error','')}")

    async def release(self):
        """Called after every mint. Standard profile: count mints, recycle the
        browser process in the background at the cadence (next mint hits the
        fresh process). Render-free profile: re-arm the idle teardown."""
        if self._idle_task is not None:
            self._idle_task.cancel()
            self._idle_task = None
        if RENDER_FREE:
            if IDLE_KILL_S <= 0:
                return

            async def killer():
                await asyncio.sleep(IDLE_KILL_S)
                async with self.lock:
                    try:
                        if self._browser is not None and self._browser.is_connected():
                            await self._browser.close()
                    except Exception:
                        pass
                    self._browser = None

            self._idle_task = asyncio.create_task(killer())
            return

        self.mints_since_launch += 1
        if RECYCLE_MINTS and self.mints_since_launch >= RECYCLE_MINTS:
            self._stats["recycles"] += 1
            _log(f"recycle after {self.mints_since_launch} mints (cadence {RECYCLE_MINTS})")
            async with self.lock:
                try:
                    if self._browser is not None and self._browser.is_connected():
                        await self._browser.close()
                except Exception:
                    pass
                self._browser = None
            self.warm_in_background()

    async def shutdown(self):
        async with self.lock:
            try:
                while True:
                    ctx = self._ctx_pool.get_nowait()
                    try:
                        await ctx.close()
                    except Exception:
                        pass
            except asyncio.QueueEmpty:
                pass
            try:
                if self._browser is not None and self._browser.is_connected():
                    await self._browser.close()
            except Exception:
                pass
            self._browser = None
            try:
                if self._pw is not None:
                    await self._pw.stop()
                    self._pw = None
            except Exception:
                pass

    async def context(self):
        """Legacy accessor (kept for external tooling/tests)."""
        return await self._new_context()


POOL = BrowserPool()


class BusyQueue(Exception):
    """Bounded-queue overflow: honest 429 so the lane falls through NOW."""


def _check_auth(authorization: str | None) -> None:
    if AUTH_TOKEN and authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


_inflight = 0
_queued = 0


async def _acquire_slot() -> None:
    global _inflight, _queued
    _queued += 1
    try:
        await asyncio.wait_for(POOL.sem.acquire(), timeout=QUEUE_WAIT_S)
    except asyncio.TimeoutError:
        _stats["busy_429"] += 1
        raise BusyQueue(
            f"solver busy: {MAX_CONCURRENT} mint(s) in flight and the "
            f"{QUEUE_WAIT_S}s queue wait elapsed — retry or fall through"
        )
    finally:
        _queued -= 1
    _inflight += 1


def _release_slot() -> None:
    global _inflight
    _inflight = max(0, _inflight - 1)
    POOL.sem.release()


def _shape_ok(token: str, sitekey: str = "") -> bool:
    # Cloudflare's DOCUMENTED dummy sitekeys (1x/2x/3x000…) deliberately mint
    # short dummy tokens — they verify against the matching dummy secrets and
    # are meant for exactly this kind of smoke test. Only production keys get
    # the strict shape gate.
    if sitekey[:4] in ("1x00", "2x00", "3x00"):
        return len(token) >= 20
    return bool(TOKEN_RE.match(token)) or bool(TOKEN_FALLBACK_RE.match(token))


async def _reader(page) -> dict:
    try:
        return await page.evaluate(
            """() => {
                const i = document.querySelector('input[name="cf-turnstile-response"]');
                return { tok: (i && i.value) || window._bxresult || null,
                         err: window._bxerr || null };
            }"""
        )
    except Exception:
        return {}


async def _frame_diag(page) -> str:
    """The Turnstile iframe's own error text ("Could not load the check…" etc.)
    plus any error-callback value — this is what makes live-fire triage
    possible from the lane's honest-fail notes."""
    bits: list[str] = []
    try:
        for f in page.frames:
            if "challenges.cloudflare.com" in (f.url or ""):
                txt = await f.evaluate(
                    "() => document.body ? document.body.innerText.replace(/\\s+/g,' ').slice(0,180) : ''"
                )
                if txt:
                    bits.append(f"widget: {txt}")
                break
    except Exception:
        pass
    try:
        err = await page.evaluate("() => window._bxerr || null")
        if err:
            bits.append(f"error-callback: {err}")
    except Exception:
        pass
    return " | ".join(bits[-2:]) if bits else ""


async def _click_checkbox(page) -> bool:
    try:
        for f in page.frames:
            if "challenges.cloudflare.com" in (f.url or ""):
                await f.click("input[type='checkbox']", timeout=2500)
                return True
    except Exception:
        pass
    return False


# ------------------------------------------------------------- generic /solve
# NOTE: turnstile.render() callbacks must be REAL function expressions —
# json.dumps would stringify them, so the object literal is templated with
# only the string fields JSON-escaped.
MINT_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>…</title>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit" async></script>
</head><body><div id="bx"></div><input name="cf-turnstile-response" type="hidden" value="">
<script>
window._bxresult = null; window._bxerr = null;
(function poll() {
  if (window.turnstile && window.turnstile.render) {
    try {
      window.turnstile.render("#bx", {
        sitekey: __SITEKEY__,
__EXTRA_FIELDS__
        callback: function (t) {
          window._bxresult = t;
          var i = document.querySelector('input[name="cf-turnstile-response"]');
          if (i) i.value = t;
        },
        "error-callback": function (e) { window._bxerr = String(e); },
        "expired-callback": function () { window._bxresult = null; },
        retry: "never",
        appearance: "always"
      });
    } catch (e) { window._bxerr = String(e); }
    return;
  }
  setTimeout(poll, 100);
})();
</script></body></html>"""


async def mint_solve(page, url: str, sitekey: str, action: str, cdata: str, budget_ms: int):
    """Render the minimal widget page AT THE REAL ORIGIN of `url`: the document
    request is fulfilled locally, so the browser still talks to the real site
    for the challenge network calls and the token carries the right hostname —
    while WE control the widget config (exact sitekey/action/cdata binding).

    Returns (token|None, diag, fail_class). fail_class "" = success or plain
    transient (the driver decides)."""
    extra = ""
    if action:
        extra += f"        action: {json.dumps(action[:32])},\n"
    if cdata:
        extra += f"        cdata: {json.dumps(cdata[:255])},\n"
    html = (MINT_HTML
            .replace("__SITEKEY__", json.dumps(sitekey))
            .replace("__EXTRA_FIELDS__", extra))

    # Challenge-transport observation: a 4xx on the fo submission is a DEAD
    # challenge (deterministic) — record it so the loop can abort early.
    fo_status: list[int] = []

    def on_response(r):
        try:
            u = r.url or ""
            if "challenge-platform" in u and "/fo/" in u and r.status >= 400:
                fo_status.append(r.status)
        except Exception:
            pass

    page.on("response", on_response)

    async def fulfill_document(route):
        if route.request.resource_type == "document" or route.request.url == url:
            await route.fulfill(status=200, content_type="text/html", body=html)
        else:
            await route.continue_()

    await page.route("**/*", fulfill_document)
    try:
        await page.goto(url, wait_until="load", timeout=min(budget_ms, 30_000))
    except Exception:
        # goto may outlive "load" on slow frames — the route already served
        # our page, so continue to the mint loop regardless.
        pass
    finally:
        try:
            await page.unroute("**/*")
        except Exception:
            pass

    deadline = time.time() + budget_ms / 1000
    clicks = 0
    while time.time() < deadline:
        if fo_status:
            return None, f"fo-reject: fingerprint submission answered HTTP {fo_status[0]}", "fo-reject"
        guard = _rss_guard_diag()
        if guard:
            return None, guard, "rss-guard"
        rd = await _reader(page)
        tok = rd.get("tok")
        if isinstance(tok, str) and tok:
            if _shape_ok(tok, sitekey):
                return tok, "", ""
            return None, f"shape mismatch len={len(tok)}", "shape"
        if rd.get("err") and not tok:
            diag = await _frame_diag(page)
            return None, diag or f"error-callback: {rd.get('err')}", "widget-error"
        frac = 1 - (deadline - time.time()) * 1000 / budget_ms
        if clicks < len(CLICK_AT_FRACTIONS) and frac >= CLICK_AT_FRACTIONS[clicks]:
            clicks += 1
            await _click_checkbox(page)
        await asyncio.sleep(0.5)
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass
    diag = await _frame_diag(page)
    if diag or clicks:
        # the challenge loaded and we interacted — that is the measured
        # client-class stall signature (pat refusal / silent completion wall)
        return None, diag or f"interactive challenge ran ({clicks} click(s)), no mint", "interactive-stall"
    return None, diag or "budget exhausted before mint", "transient"


# --------------------------------------------------------------- page-walk mint
async def mint_walk(page, url: str, budget_ms: int):
    """Load the REAL captcha page and let the page's own widget mint — binding
    and hostname honored by construction. Mirrors the proven scrape.do walk
    (settle waits + click cadence + input reader + HTML regex fallback).

    Returns (token|None, diag, fail_class)."""
    fo_status: list[int] = []

    def on_response(r):
        try:
            u = r.url or ""
            if "challenge-platform" in u and "/fo/" in u and r.status >= 400:
                fo_status.append(r.status)
        except Exception:
            pass

    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=min(budget_ms, 45_000))
    except Exception:
        pass  # the page is up enough for the widget in the common case

    deadline = time.time() + budget_ms / 1000
    clicks = 0
    while time.time() < deadline:
        if fo_status:
            return None, f"fo-reject: fingerprint submission answered HTTP {fo_status[0]}", "fo-reject"
        guard = _rss_guard_diag()
        if guard:
            return None, guard, "rss-guard"
        rd = await _reader(page)
        tok = rd.get("tok")
        if isinstance(tok, str) and tok:
            if _shape_ok(tok):
                return tok, "", ""
            return None, f"shape mismatch len={len(tok)}", "shape"
        try:
            html = await page.content()
            m = TOKEN_FALLBACK_RE.search(html)
            if m and _shape_ok(m.group(0)):
                return m.group(0), "", ""
        except Exception:
            pass
        frac = 1 - (deadline - time.time()) * 1000 / budget_ms
        if clicks < len(CLICK_AT_FRACTIONS) and frac >= CLICK_AT_FRACTIONS[clicks]:
            clicks += 1
            await _click_checkbox(page)
        await asyncio.sleep(0.5)
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass
    diag = await _frame_diag(page)
    if diag or clicks:
        return None, diag or f"interactive challenge ran ({clicks} click(s)), no mint", "interactive-stall"
    return None, diag or "budget exhausted before mint", "transient"


# -------------------------------------------------------------- mint driver
async def _run_mint(kind: str, payload: dict, budget_ms: int):
    """Shared mint driver: doom-cache check, warm context from the pool,
    budget-split retry on transient/driver failures, honest failClass.
    Returns (token|None, diag, fail_class, attempts, fastfailed: bool)."""
    from urllib.parse import urlparse

    sitekey = str(payload.get("sitekey") or "")
    if not sitekey:
        # walk mode carries no sitekey — doom-key by the target host instead
        # (the lootlabs walk family all share one host; the classifier bucket
        # then matches the lane's view of the world)
        try:
            sitekey = (urlparse(str(payload.get("url") or "")).hostname or "").lower()
        except Exception:
            sitekey = ""
    proxy_override = payload.get("proxy") if isinstance(payload.get("proxy"), dict) else None

    doom = _doom_check(sitekey) if sitekey else None
    if doom:
        _stats["fastfails"] += 1
        rec = _sitekey_rec(sitekey)
        rec["fastfails"] += 1
        return (
            None,
            f"doom-cached ({doom['failClass']}): {ADVICE['doom-cached']}",
            "doom-cached",
            0,
            True,
            sitekey,
        )

    attempt_budget_ms = max(4_000, int(budget_ms * RETRY_SPLIT))
    remaining_ms = budget_ms
    attempts = 0
    token, diag, fail_class = None, "", ""
    while True:
        attempts += 1
        t_try = time.time()
        ctx = None
        try:
            await POOL.browser()
            if proxy_override:
                ctx = await POOL._new_context(proxy_override=proxy_override)
            else:
                ctx = await POOL.borrow_context()
            page = await ctx.new_page()
            if kind == "solve":
                token, diag, fail_class = await mint_solve(
                    page,
                    str(payload.get("url") or "https://example.com/"),
                    sitekey,
                    str(payload.get("action") or ""),
                    str(payload.get("cdata") or ""),
                    attempt_budget_ms,
                )
            else:
                token, diag, fail_class = await mint_walk(
                    page, str(payload.get("url") or ""), attempt_budget_ms
                )
        except Exception as e:
            diag = f"driver: {type(e).__name__}: {str(e)[:110]}"
            fail_class = "driver"
            token = None
        finally:
            if ctx is not None:
                POOL.return_context(ctx)
            _note_rss()
        used_ms = int((time.time() - t_try) * 1000)
        remaining_ms -= used_ms
        if token:
            return token, diag, "", attempts, False, sitekey
        retryable = fail_class in ("transient", "driver", "") or (not diag and not fail_class)
        can_retry = (
            RETRY_ENABLED
            and attempts == 1
            and remaining_ms > 8_000
            and retryable
        )
        if not can_retry:
            return None, diag, fail_class or "transient", attempts, False, sitekey
        _stats["retries"] += 1
        _log(f"retry {kind}: {fail_class or 'dry'} ({diag[:80]}) — {remaining_ms}ms left")
        attempt_budget_ms = max(4_000, remaining_ms - 1_000)


def _finish(kind: str, ok: bool, ms: int, diag: str, attempts: int, extra: dict, fail_class: str, sitekey: str):
    _stats["last_ms"] = ms
    if diag:
        _stats["last_diag"] = diag[:200]
    if ok:
        _ms_window.append(ms)
        _stats["avg_ms"] = int(sum(_ms_window) / len(_ms_window)) if _ms_window else 0
        _stats["p50_ms"] = _pctl(_ms_window, 0.5)
        _stats["p95_ms"] = _pctl(_ms_window, 0.95)
        key = f"{kind}s_ok"
        _stats[key] = _stats.get(key, 0) + 1
    else:
        key = f"{kind}s_fail"
        _stats[key] = _stats.get(key, 0) + 1
        _stats["last_fail_class"] = fail_class
        if diag:
            _stats["last_error"] = diag[:200]
    _classify_and_remember(sitekey, fail_class, ok, ms)
    _recent_diags.append({
        "t": int(time.time()), "kind": kind, "ok": ok, "ms": ms,
        "class": fail_class, "sitekey": sitekey[:24], "diag": diag[:140],
    })
    _log(f"{kind} ok={int(ok)} ms={ms} attempts={attempts} class={fail_class or '-'} diag={diag[:110] if diag else '-'}")
    return extra


def _sitekeys_public() -> dict:
    out = {}
    for sk, rec in _sitekeys.items():
        out[sk] = {
            "ok": rec["ok"], "fail": rec["fail"], "fastfails": rec["fastfails"],
            "classes": dict(rec["classes"]), "last_ms": rec["last_ms"],
            "p50_ms": rec["p50_ms"], "p95_ms": rec["p95_ms"],
            "doom_streak": rec["doom_streak"],
        }
    return out


# -------------------------------------------------------------------- endpoints
@asynccontextmanager
async def lifespan(_app: FastAPI):
    if WARMUP:
        POOL.warm_in_background()
    yield
    await POOL.shutdown()


app = FastAPI(title="bx-solver", docs_url=None, redoc_url=None, lifespan=lifespan)


async def _browser_state() -> str:
    try:
        if POOL._browser is not None and POOL._browser.is_connected():
            return "up"
    except Exception:
        pass
    return "warming" if (POOL.warming or (POOL._warm_task is not None and not POOL._warm_task.done())) else "down"


@app.get("/health")
async def health():
    now = time.time()
    return {
        "ok": True,
        "service": "bx-solver",
        "version": VERSION,
        "profile": "render-free" if RENDER_FREE else "standard",
        "browser": await _browser_state(),
        "browser_age_s": int(now - POOL.launched_at) if POOL.launched_at else 0,
        "headless": HEADLESS,
        "brunhild_pin": BRUNHILD_PIN,
        "max_concurrent": MAX_CONCURRENT,
        "queue_wait_s": QUEUE_WAIT_S,
        "retry": RETRY_ENABLED,
        "recycle_mints": RECYCLE_MINTS,
        "mints_since_launch": POOL.mints_since_launch,
        "ctx_pool": POOL._ctx_pool.qsize(),
        "ctx_pool_size": POOL_SIZE,
        "inflight": _inflight,
        "queued": _queued,
        "doom_after": DOOM_AFTER,
        "doom_ttl_s": DOOM_TTL_S,
        "doomed": {sk: {"failClass": d["failClass"], "expires_in_s": int(d["until"] - now)} for sk, d in _doom.items()},
        "proxy": bool(DEFAULT_PROXY),
        "rss_mb": _tree_rss_mb(),
        "rss_guard_mb": RSS_ABORT_MB,
        "uptime_s": int(now - _START),
        "stats": _stats,
        "sitekeys": _sitekeys_public(),
        "recent": list(_recent_diags),
    }


@app.get("/warm")
@app.post("/warm")
async def warm(authorization: str | None = Header(default=None)):
    _check_auth(authorization)
    r = await POOL.warm()
    _note_rss()
    return r


async def _mint_endpoint(kind: str, payload: dict, authorization: str | None):
    _check_auth(authorization)
    if kind == "solve":
        if not str(payload.get("sitekey") or "").strip():
            return JSONResponse({"ok": False, "error": "sitekey required"}, status_code=400)
        budget = min(int(payload.get("budgetMs") or 45_000), SOLVE_BUDGET_CAP_MS)
    else:
        if not str(payload.get("url") or "").strip():
            return JSONResponse({"ok": False, "error": "url required"}, status_code=400)
        budget = min(int(payload.get("budgetMs") or 60_000), WALK_BUDGET_CAP_MS)

    t0 = time.time()
    try:
        await _acquire_slot()
    except BusyQueue as e:
        return JSONResponse(
            {"ok": False, "error": str(e), "ms": int((time.time() - t0) * 1000), "mode": kind},
            status_code=429,
        )
    try:
        token, diag, fail_class, attempts, fastfailed, doom_key = await _run_mint(kind, payload, budget)
    finally:
        _release_slot()
        if RENDER_FREE:
            gc.collect()
    ms = int((time.time() - t0) * 1000)
    if token:
        return _finish(kind, True, ms, "", attempts, {
            "ok": True, "token": token, "ms": ms, "mode": kind, "attempts": attempts,
        }, "", doom_key)
    body = {
        "ok": False,
        "error": diag or "mint failed",
        "failClass": fail_class,
        "advice": ADVICE.get(fail_class, ""),
        "ms": ms,
        "mode": kind,
        "attempts": attempts,
    }
    return _finish(kind, False, ms, diag, attempts, body, fail_class, doom_key)


@app.post("/solve")
async def solve(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
):
    return await _mint_endpoint("solve", payload, authorization)


@app.post("/walk")
async def walk(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
):
    return await _mint_endpoint("walk", payload, authorization)


if __name__ == "__main__":
    import uvicorn

    _log(
        f"bx-solver v{VERSION} listening on {HOST}:{PORT} "
        f"(profile={'render-free' if RENDER_FREE else 'standard'}, headless={HEADLESS}, "
        f"brunhild_pin={BRUNHILD_PIN}, max_concurrent={MAX_CONCURRENT}, warmup={WARMUP}, "
        f"queue_wait_s={QUEUE_WAIT_S}, retry={RETRY_ENABLED}, "
        f"recycle_mints={RECYCLE_MINTS or 'off'}, rss_guard_mb={RSS_ABORT_MB or 'off'}, "
        f"pool={POOL_SIZE}, doom_after={DOOM_AFTER or 'off'} ttl={DOOM_TTL_S}s, "
        f"proxy={'yes' if DEFAULT_PROXY else 'no'})"
    )
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
