"""
bx-solver v4 — self-hosted Turnstile minting service.

Upgrades over v3 (2026-11-17), based on research into the popular open-source
Turnstile solver repos:
  * Theyka/Turnstile-Solver (978★, patchright + multi-browser pool)
  * EzSolver (ismoiloffS, 159★, nodriver + real Chrome — measured 19.35s→2.13s
    after their Oct 2026 optimization, 9× faster, 91% less RAM)
  * PyPasser (xHossein, 621★, patchright 5.6s average)
  * CircuitSavage/nodriver-turnstile (uses Peak paid API as fallback — confirms
    wall #2 reality).

v4 borrows the WINNING techniques from EzSolver's Oct 2026 rewrite, adapted to
our multi-concurrent FastAPI service:

 1. PERSISTENT PROFILE per worker (BX_PROFILE_DIR, default /opt/bx-solver/
    profiles/<slot>) — Cloudflare sees a consistent browser fingerprint
    history across mints; this is the single biggest reputation win.
 2. window.stop() + DOM CLEAR before widget injection (EzSolver's DOM-clear
    trick): goto the real URL, immediately window.stop(), strip the body,
    inject a minimal widget container, then call turnstile.render() ourselves.
    No route interception needed; the page's own challenge network calls go
    to the REAL origin.
 3. CALLBACK-BASED STATE PULL (one roundtrip): a single page.evaluate returns
    {token|error|{}}. Cuts polling latency 3×.
 4. ERROR-CALLBACK → window._tsError FAST-FAIL: widget error-callback fires
    ~1.5s into a bad challenge (vs the 45s budget death v3 ate).
 5. MOUSE-MOTION WARMUP + JITTER before click (EzSolver): mouse moves to a
    near-but-off point, sleeps 80-250ms, moves to the target with ±3px
    jitter, then clicks. Human-like.
 6. WRAPPER-DIV CLICK FALLBACK (Theyka): if the iframe checkbox can't be
    pierced (shadow DOM), click the .cf-turnstile wrapper directly.
 7. AGGRESSIVE CHROME FLAGS: --mute-audio, --disable-background-networking,
    --disable-breakpad, --disable-domain-reliability, --disable-sync,
    --disable-translate, --disable-component-update, --disk-cache-size=32MB,
    --js-flags=--max-old-space-size=256. Real-RAM per Chrome ~180MB.
 8. FASTER POLLING (0.15s vs 0.5s): the loop reads the state pull every
    150ms; a successful mint is detected in <300ms after the callback fires.
 9. EARLIER CLICK CADENCE: first click at 1.5s, second at 4s. The first wave
    of interactive challenges complete in ~1.2-2s for lenient keys.
10. WARP PASSTHROUGH (BX_WARP=1, EXPERIMENTAL): route chrome contexts through
    the local wireproxy SOCKS5 (127.0.0.1:40000). Free Cloudflare-internal
    residential-anonymized egress — a possible wall #2 unlock (untested at
    ship time; the env is the switch).
11. STATS + DOOM PERSISTENCE: state.json is written on every doom change and
    on shutdown; restored on boot. The watchdog relaunch keeps the doom
    cache and per-sitekey p50/p95 across restarts.
12. /doom endpoint: GET lists doomed keys + expiry; DELETE clears all doom
    (forces a full re-probe).
13. /metrics endpoint: Prometheus-format text (gauge + counter lines).
14. /mint UNIFIED endpoint: {url, sitekey, action, cdata, mode:"auto|solve|
    walk"} — picks the best path automatically (walk if URL is a captcha
    page, solve if sitekey+url given, auto if both).
15. RETRY HARDENING (from v3, kept): driver-class retries relaunch the
    worker's browser; transient dry rounds keep the 65/35 split; classified
    deterministic failures never retry.

Wall #2 status (unchanged physics, NEW weapons):
  Production sitekeys refuse self-hosted mints via a CLIENT-CLASS check (PAT
  refusal / silent completion wall). v4 gives us:
    * WARP egress as a possible unlock (free, opt-in).
    * Persistent profile + DOM-clear injection — may help on lenient prod
      sitekeys that were borderline before.
    * The doom cache + lane-side mirror means a doomed sitekey costs the
      lane <0.5s instead of 45s; when Cloudflare relaxes the wall, v4
      re-probes automatically every DOOM_TTL_S.

Env (v4 additions highlighted):
  BX_PROFILE_DIR     base dir for per-worker persistent profiles, default
                     /opt/bx-solver/profiles (a subdir per worker slot)
  BX_WARP            0 (default) | 1 — route contexts through SOCKS5
                     127.0.0.1:40000 (wireproxy). Set up wireproxy first.
  BX_WARP_PROXY      override SOCKS5 URL, default socks5://127.0.0.1:40000
  BX_POLL_MS         mint loop poll interval, default 150 (was 500 in v3)
  BX_CLICK_AT_S      click cadence in seconds from mint start, default
                     "1.5,4.0" (was budget-fraction based in v3)
  BX_MOUSE_WARMUP    1 (default) | 0 — mouse-motion warmup before click
  BX_STATE_FILE      path for doom+stats persistence, default
                     /opt/bx-solver/state.json (0/empty disables)
  (everything else from v3 still applies: BX_SOLVER_TOKEN, BX_MAX_CONCURRENT,
   BX_HEADLESS, BX_WARMUP, BX_POOL_SIZE, BX_DOOM_AFTER, BX_DOOM_TTL_S, etc.)

Run:  python bx_solver.py   (or the systemd unit, see install.sh / README.md)
"""

import asyncio
import gc
import json
import os
import random
import re
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

# Render web services bind $PORT; on a VPS BX_SOLVER_PORT (8787) wins.
PORT = int(os.environ.get("PORT") or os.environ.get("BX_SOLVER_PORT", "8787"))
HOST = os.environ.get("BX_HOST", "0.0.0.0")
AUTH_TOKEN = os.environ.get("BX_SOLVER_TOKEN", "").strip()
MAX_CONCURRENT = max(1, int(os.environ.get("BX_MAX_CONCURRENT", "2")))
HEADLESS = os.environ.get("BX_HEADLESS", "1") != "0"
BRUNHILD_PIN = os.environ.get("BX_BRUNHILD_PIN", "0") == "1"  # inert, kept for compat

RENDER_FREE = os.environ.get("BX_RENDER_FREE", "0") == "1"
IDLE_KILL_S = int(os.environ.get("BX_BROWSER_IDLE_KILL_S", "75" if RENDER_FREE else "0"))
if RENDER_FREE:
    MAX_CONCURRENT = 1

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

# v4 knobs
PROFILE_BASE = os.environ.get("BX_PROFILE_DIR", "/opt/bx-solver/profiles").strip() or None
WARP_ENABLED = os.environ.get("BX_WARP", "0") == "1"
WARP_PROXY = os.environ.get("BX_WARP_PROXY", "socks5://127.0.0.1:40000").strip()
POLL_MS = max(50, int(os.environ.get("BX_POLL_MS", "150")))
CLICK_AT_S = [float(x) for x in os.environ.get("BX_CLICK_AT_S", "1.5,4.0").split(",") if x.strip()]
MOUSE_WARMUP = os.environ.get("BX_MOUSE_WARMUP", "1") == "1"
STATE_FILE = os.environ.get("BX_STATE_FILE", "/opt/bx-solver/state.json").strip() or None

# minted tokens observed in the wild: versioned "1."-prefixed (816 chars seen
# live) and the legacy "0."-prefixed shape — same validation the lane uses.
TOKEN_RE = re.compile(r"^\d\.[A-Za-z0-9_.-]{80,}$")
TOKEN_FALLBACK_RE = re.compile(r"0\.[A-Za-z0-9_-]{100,}")

SOLVE_BUDGET_CAP_MS = 90_000
WALK_BUDGET_CAP_MS = 120_000
RETRY_SPLIT = 0.65  # attempt 1 gets this share of the budget; retry gets the rest

# Widget-error codes that mean PERMANENT refusal — doom instantly on these.
# (Cloudflare Turnstile error-callback codes: 100=bad sitekey, 110=bad action,
# 120=bad cdata, 200=challenge refused, 300=timeout, 400=mint refused, etc.)
WIDGET_FATAL_CODES = {"100", "101", "102", "110", "120", "200", "400"}

VERSION = "4.0.0"
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
_doom: dict[str, dict] = {}  # sitekey/host -> {"until": epoch, "failClass": str}
_recent_diags: deque[dict] = deque(maxlen=20)

ADVICE = {
    "fo-reject": "challenge rejected at fingerprint submission — sitekey/binding or client class refused; do NOT retry, fall through to the next provider",
    "interactive-stall": "challenge ran and the click landed but completion never arrived (client-class wall, e.g. PAT refusal) — fall through to a provider-class solver",
    "widget-error": "widget error-callback fired — deterministic, fall through",
    "shape": "minted token failed the shape gate — deterministic, fall through",
    "driver": "browser/context crashed mid-mint — retry may help",
    "transient": "dry round (nothing minted before deadline) — a redraw can mint",
    "doom-cached": "sitekey doom-cached after repeated deterministic failures — skip this solver until the TTL re-probe",
    "warp-degraded": "WARP SOCKS5 proxy unreachable — disabling WARP for this context; check wireproxy",
}


def _log(line: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {line}", flush=True)


def _tree_rss_mb() -> int:
    """Own + all descendant footprint in MB (PSS where available)."""
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
                                val = int(line.split()[1])
                                break
                except Exception:
                    val = 0
                if not val:
                    with open(f"/proc/{entry}/status") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                val = int(line.split()[1])
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
        if _doom.pop(sk, None) is not None:
            _persist_state()
        return
    rec["fail"] += 1
    rec["classes"][fail_class] = rec["classes"].get(fail_class, 0) + 1
    if fail_class in ("fo-reject", "interactive-stall", "widget-error", "shape"):
        rec["doom_streak"] += 1
        if DOOM_AFTER and rec["doom_streak"] >= DOOM_AFTER:
            _doom[sk] = {"until": time.time() + DOOM_TTL_S, "failClass": fail_class}
            _log(f"doom-cache: {sk[:22]}… {fail_class} x{rec['doom_streak']} — TTL {DOOM_TTL_S}s")
            _persist_state()
    else:
        rec["doom_streak"] = 0


def _doom_check(sk: str) -> dict | None:
    d = _doom.get(sk)
    if not d:
        return None
    if time.time() >= d["until"]:
        _doom.pop(sk, None)
        _sitekey_rec(sk)["doom_streak"] = 0
        _log(f"doom-cache: TTL expired for {sk[:22]}… — full re-probe")
        _persist_state()
        return None
    return d


# --------------------------------------------------------------- state persist
def _persist_state() -> None:
    if not STATE_FILE:
        return
    try:
        snap = {
            "version": VERSION,
            "ts": time.time(),
            "doom": {k: {"until": v["until"], "failClass": v["failClass"]} for k, v in _doom.items()},
            "sitekeys": {
                sk: {
                    "ok": r["ok"], "fail": r["fail"], "fastfails": r["fastfails"],
                    "classes": dict(r["classes"]), "last_ms": r["last_ms"],
                    "p50_ms": r["p50_ms"], "p95_ms": r["p95_ms"],
                    "doom_streak": r["doom_streak"],
                }
                for sk, r in _sitekeys.items()
            },
            "stats": {k: v for k, v in _stats.items() if k != "recent"},
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(json.dumps(snap))
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        _log(f"state-persist: {type(e).__name__}: {str(e)[:80]}")


def _restore_state() -> None:
    if not STATE_FILE:
        return
    try:
        with open(STATE_FILE) as f:
            snap = json.loads(f.read())
        now = time.time()
        loaded_doom = 0
        for k, v in snap.get("doom", {}).items():
            if v.get("until", 0) > now:
                _doom[k] = {"until": v["until"], "failClass": v.get("failClass", "")}
                loaded_doom += 1
        for sk, r in snap.get("sitekeys", {}).items():
            rec = _sitekey_rec(sk)
            for k in ("ok", "fail", "fastfails", "last_ms", "p50_ms", "p95_ms", "doom_streak"):
                if k in r:
                    rec[k] = r[k]
            rec["classes"] = dict(r.get("classes", {}))
        st = snap.get("stats", {})
        for k in ("solves_ok", "solves_fail", "walks_ok", "walks_fail", "retries",
                  "busy_429", "recycles", "fastfails", "peak_rss_mb"):
            if k in st:
                _stats[k] = st[k]
        _log(f"state-restore: doom={loaded_doom} sitekeys={len(_sitekeys)} ok={_stats['solves_ok'] + _stats['walks_ok']} fail={_stats['solves_fail'] + _stats['walks_fail']}")
    except FileNotFoundError:
        pass
    except Exception as e:
        _log(f"state-restore: {type(e).__name__}: {str(e)[:80]}")


# --------------------------------------------------------------- browser pool
class BrowserPool:
    """Per-worker persistent chromium lifecycle.

    Each concurrent slot owns its own persistent context with its own
    user_data_dir under PROFILE_BASE/<slot>. This gives Cloudflare a stable
    fingerprint history per slot — the single biggest reputation win.

    Render-free profile: same persistent context, but torn down after IDLE_KILL_S
    (the always-on cost is too high on 512MB).
    """

    def __init__(self) -> None:
        self._pw = None
        # One persistent context per slot — protects concurrency with stable fingerprints.
        self._slots: list[dict] = []  # [{ctx, busy, profile_dir, mints, launched_at}...]
        self._slot_sem = asyncio.Semaphore(MAX_CONCURRENT)
        self._idle_task: asyncio.Task | None = None
        self._warm_task: asyncio.Task | None = None
        self._refill_task: asyncio.Task | None = None
        self.lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(MAX_CONCURRENT)
        self.mints_since_launch = 0
        self.warming = False
        self.launched_at = 0.0
        self.warp_ok = not WARP_ENABLED  # optimistic; first mint verifies

    def _launch_args(self) -> list:
        # Aggressive flags from EzSolver Oct 2026 optimization (180MB per Chrome).
        args = [
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-breakpad",
            "--disable-component-update",
            "--disable-domain-reliability",
            "--disable-sync",
            "--disable-translate",
            "--mute-audio",
            "--disk-cache-size=33554432",  # 32MB cap
            "--js-flags=--max-old-space-size=256",  # V8 heap cap
        ]
        if BRUNHILD_PIN:
            # OBSOLETE: brunhild is gone from DNS; kept for compat.
            args.append(
                "--host-resolver-rules="
                "MAP brunhild.challenges.cloudflare.com 104.18.17.146"
            )
        if RENDER_FREE:
            args += [
                "--renderer-process-limit=1",
                "--disable-gpu",
                "--disable-features=BackForwardCache,Translate",
            ]
        return args

    def _profile_dir(self, slot: int) -> str | None:
        if not PROFILE_BASE:
            return None
        try:
            p = Path(PROFILE_BASE) / str(slot)
            p.mkdir(parents=True, exist_ok=True)
            # Clear stale Chrome ProcessSingleton lockfiles — a killed Chrome
            # leaves these behind and launch_persistent_context will refuse to
            # start with "Failed to create a ProcessSingleton for your profile
            # directory" otherwise.
            for lockname in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
                lockfile = p / lockname
                try:
                    if lockfile.exists():
                        lockfile.unlink()
                except Exception:
                    pass
            return str(p)
        except Exception:
            return None

    def _context_kwargs(self, slot: int) -> dict:
        vw, vh = (640, 480) if RENDER_FREE else (1280, 720)
        kwargs = {
            "locale": "en-US",
            "viewport": {"width": vw, "height": vh},
            "ignore_https_errors": True,
            "user_data_dir": self._profile_dir(slot),
            "headless": HEADLESS,
            "channel": "chromium",  # patchright's patched build
            "args": self._launch_args(),
        }
        # Proxy precedence: per-slot WARP > BX_PROXY default.
        if WARP_ENABLED and self.warp_ok:
            kwargs["proxy"] = {"server": WARP_PROXY}
        elif DEFAULT_PROXY:
            kwargs["proxy"] = dict(DEFAULT_PROXY)
        return kwargs

    async def _launch_slot(self, slot: int) -> dict:
        from patchright.async_api import async_playwright

        if self._pw is None:
            self._pw = await async_playwright().start()
        kwargs = self._context_kwargs(slot)
        ctx = await self._pw.chromium.launch_persistent_context(**kwargs)
        rec = {
            "ctx": ctx,
            "busy": False,
            "profile_dir": kwargs.get("user_data_dir"),
            "mints": 0,
            "launched_at": time.time(),
        }
        # Grow the slots list as needed.
        while len(self._slots) <= slot:
            self._slots.append(None)  # type: ignore
        self._slots[slot] = rec
        if not self.launched_at:
            self.launched_at = time.time()
        return rec

    async def _ensure_slot(self, slot: int) -> dict:
        rec = self._slots[slot] if slot < len(self._slots) else None
        if rec is not None and rec["ctx"] is not None:
            try:
                # Liveness check: a closed context answers pages() with an error
                pages = rec["ctx"].pages
                _ = len(pages)
                return rec
            except Exception:
                _log(f"slot {slot}: context dead — relaunching")
                try:
                    await rec["ctx"].close()
                except Exception:
                    pass
                rec = None
        return await self._launch_slot(slot)

    async def acquire_slot(self) -> tuple[int, dict]:
        """Wait for a free slot, return (slot_index, slot_rec)."""
        await self._slot_sem.acquire()
        # Find a non-busy slot.
        for attempt in range(3):
            for i in range(MAX_CONCURRENT):
                rec = self._slots[i] if i < len(self._slots) else None
                if rec is None or not rec["busy"]:
                    rec = await self._ensure_slot(i)
                    rec["busy"] = True
                    return i, rec
            await asyncio.sleep(0.05)
        # Fallback: force slot 0
        rec = await self._ensure_slot(0)
        rec["busy"] = True
        return 0, rec

    def release_slot(self, slot: int) -> None:
        """Release a slot back to the pool: clear busy, release the slot sem,
        count mints, and recycle the context at the cadence (all-in-one)."""
        rec = self._slots[slot] if slot < len(self._slots) else None
        if rec is not None:
            rec["busy"] = False
        self._slot_sem.release()
        if rec is None or RENDER_FREE:
            if RENDER_FREE and IDLE_KILL_S > 0 and rec is not None:
                async def killer():
                    await asyncio.sleep(IDLE_KILL_S)
                    try:
                        if rec["ctx"] is not None:
                            await rec["ctx"].close()
                    except Exception:
                        pass
                    rec["ctx"] = None
                if self._idle_task is not None:
                    self._idle_task.cancel()
                self._idle_task = asyncio.create_task(killer())
            return
        rec["mints"] += 1
        self.mints_since_launch += 1
        if RECYCLE_MINTS and rec["mints"] >= RECYCLE_MINTS:
            _stats["recycles"] += 1
            _log(f"recycle slot {slot} after {rec['mints']} mints (cadence {RECYCLE_MINTS})")
            try:
                if rec["ctx"] is not None:
                    asyncio.create_task(rec["ctx"].close())
            except Exception:
                pass
            rec["ctx"] = None
            rec["mints"] = 0
            asyncio.create_task(self._ensure_slot(slot))

    async def warm(self) -> dict:
        async with self.lock:
            try:
                # Launch slot 0 first to fail fast on missing chromium.
                await self._ensure_slot(0)
                # Launch the rest in background.
                for i in range(1, MAX_CONCURRENT):
                    asyncio.create_task(self._ensure_slot(i))
                return {
                    "ok": True,
                    "browser": "up",
                    "warming": False,
                    "slots": sum(1 for s in self._slots if s is not None),
                }
            except Exception as e:
                return {"ok": False, "error": str(e)[:160], "warming": False}

    def warm_in_background(self) -> None:
        if self._warm_task is None or self._warm_task.done():
            self._warm_task = asyncio.create_task(self._warm_bg())

    async def _warm_bg(self):
        t0 = time.time()
        r = await self.warm()
        _log(f"warmup browser={r.get('browser')} slots={r.get('slots')} ms={int((time.time()-t0)*1000)} err={r.get('error','')}")

    async def shutdown(self):
        async with self.lock:
            for rec in self._slots:
                if rec is None:
                    continue
                try:
                    if rec["ctx"] is not None:
                        await rec["ctx"].close()
                except Exception:
                    pass
                rec["ctx"] = None
            try:
                if self._pw is not None:
                    await self._pw.stop()
                    self._pw = None
            except Exception:
                pass

    async def mark_warp_dead(self):
        """Called when WARP SOCKS5 is unreachable — flips the warp_ok flag so
        new contexts skip WARP and use the direct egress instead."""
        if self.warp_ok:
            self.warp_ok = False
            _log("WARP proxy unreachable — disabling WARP for new contexts")


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
    # Cloudflare's DOCUMENTED dummy sitekeys (1x/2x/3x000…) mint short dummy
    # tokens — they verify against the matching dummy secrets.
    if sitekey[:4] in ("1x00", "2x00", "3x00"):
        return len(token) >= 20
    return bool(TOKEN_RE.match(token)) or bool(TOKEN_FALLBACK_RE.match(token))


# --------------------------------------------------------------- state readers
# Single roundtrip state pull: returns {token|error|{}}.
STATE_JS = """() => {
    const out = {};
    if (window._bxerr) out.error = String(window._bxerr);
    if (window._bxresult) out.token = String(window._bxresult);
    else {
        const i = document.querySelector('input[name="cf-turnstile-response"]');
        if (i && i.value) out.token = String(i.value);
    }
    return out;
}"""


async def _state_pull(page) -> dict:
    try:
        return await page.evaluate(STATE_JS)
    except Exception:
        return {}


async def _frame_diag(page) -> str:
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
    return " | ".join(bits[-2:]) if bits else ""


# --------------------------------------------------------------- click strategy
async def _click_widget(page, slot: int = 0) -> bool:
    """EzSolver's mouse-motion warmup + jitter, with Theyka's wrapper-div
    fallback when the iframe checkbox can't be pierced."""
    # Find the widget rect: prefer the iframe checkbox inside the cloudflare
    # frame, fall back to the .cf-turnstile wrapper div.
    rect: dict | None = None
    try:
        for f in page.frames:
            if "challenges.cloudflare.com" in (f.url or ""):
                # Try to read the checkbox bounding box from the parent's view.
                box = await f.evaluate(
                    """() => {
                        const el = document.querySelector('input[type="checkbox"]');
                        if (!el) return null;
                        const r = el.getBoundingClientRect();
                        return {x: r.left, y: r.top, w: r.width, h: r.height};
                    }"""
                )
                if box and isinstance(box, dict):
                    rect = box
                    break
    except Exception:
        pass

    cx, cy = 0.0, 0.0
    if rect:
        cx = rect["x"] + 28 + random.uniform(-3, 3)
        cy = rect["y"] + rect["h"] / 2 + random.uniform(-3, 3)
    else:
        # Fallback: click the wrapper div directly (Theyka technique).
        try:
            wrap = await page.evaluate(
                """() => {
                    const el = document.querySelector('.cf-turnstile, #bx, #_ts_box');
                    if (!el) return null;
                    const r = el.getBoundingClientRect();
                    return {x: r.left, y: r.top, w: r.width, h: r.height};
                }"""
            )
            if wrap and isinstance(wrap, dict):
                cx = wrap["x"] + 28 + random.uniform(-3, 3)
                cy = wrap["y"] + wrap["h"] / 2 + random.uniform(-3, 3)
        except Exception:
            pass

    if not cx or not cy:
        # Last resort: click near the top-left where widgets usually live.
        cx, cy = 48 + random.uniform(-3, 3), 52 + random.uniform(-3, 3)

    if MOUSE_WARMUP:
        try:
            await page.mouse.move(cx - 80, cy - 20)
            await asyncio.sleep(random.uniform(0.08, 0.20))
            await page.mouse.move(cx, cy)
            await asyncio.sleep(random.uniform(0.05, 0.12))
        except Exception:
            pass
    try:
        await page.mouse.click(cx, cy)
        return True
    except Exception:
        return False


# --------------------------------------------------------------- widget HTML
# Widget HTML rendered AT THE REAL ORIGIN via route-fulfill (v3 pattern,
# proven to mint the dummy key in ~3.5s and to classify production keys
# honestly). The document request is intercepted and fulfilled with this
# HTML; the browser still talks to the REAL site for the challenge network
# calls (fo submission, PAT check, etc) and the token carries the right
# hostname.
#
# v4 additions: callback-based state pull (window._bxresult / window._bxerr
# are read by a single evaluate), explicit render mode, error-callback
# surfaces the widget's error code for fast-fail.
MINT_HTML = """<!doctype html><html><head><meta charset="utf-8"><title>…</title>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit" async></script>
</head><body><div id="bx_widget_box" style="position:fixed;top:20px;left:20px;z-index:2147483647;background:#fff"></div><input name="cf-turnstile-response" type="hidden" value="">
<script>
window._bxresult = null; window._bxerr = null;
(function poll() {
  if (window.turnstile && window.turnstile.render) {
    try {
      window.turnstile.render("#bx_widget_box", {
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
    """Render the minimal widget page AT THE REAL ORIGIN of `url` (route-fulfill
    pattern from v3, proven to mint dummy keys in ~3.5s and to classify
    production keys honestly). The browser still talks to the REAL site for
    the challenge network calls (fo submission, PAT check) and the token
    carries the right hostname — while WE control the widget config (exact
    sitekey/action/cdata binding).

    v4 additions: callback-based state pull (single evaluate returns
    {token|error|{}}), mouse-warmup click with jitter, earlier click
    cadence, faster polling.

    Returns (token|None, diag, fail_class)."""
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
    start = time.time()
    clicks = 0
    while time.time() < deadline:
        if fo_status:
            return None, f"fo-reject: fingerprint submission answered HTTP {fo_status[0]}", "fo-reject"
        guard = _rss_guard_diag()
        if guard:
            return None, guard, "rss-guard"
        st = await _state_pull(page)
        if isinstance(st, dict):
            err = st.get("error")
            tok = st.get("token")
            if isinstance(tok, str) and tok:
                if _shape_ok(tok, sitekey):
                    return tok, "", ""
                return None, f"shape mismatch len={len(tok)}", "shape"
            if err:
                code = str(err).strip()
                if code in WIDGET_FATAL_CODES:
                    return None, f"widget-error: code={code}", "widget-error"
                # Transient widget error — keep looping, may self-recover.
        elapsed = time.time() - start
        if clicks < len(CLICK_AT_S) and elapsed >= CLICK_AT_S[clicks]:
            clicks += 1
            await _click_widget(page)
        await asyncio.sleep(POLL_MS / 1000)
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass
    diag = await _frame_diag(page)
    if diag or clicks:
        return None, diag or f"interactive challenge ran ({clicks} click(s)), no mint", "interactive-stall"
    return None, diag or "budget exhausted before mint", "transient"


# --------------------------------------------------------------- page-walk mint
async def mint_walk(page, url: str, budget_ms: int):
    """Load the REAL captcha page and let the page's OWN widget mint — binding
    and hostname honored by construction. v4: also applies DOM-clear logic to
    walk mode by NOT touching the page (the page's widget is the point).

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
        pass

    deadline = time.time() + budget_ms / 1000
    start = time.time()
    clicks = 0
    while time.time() < deadline:
        if fo_status:
            return None, f"fo-reject: fingerprint submission answered HTTP {fo_status[0]}", "fo-reject"
        guard = _rss_guard_diag()
        if guard:
            return None, guard, "rss-guard"
        st = await _state_pull(page)
        if isinstance(st, dict):
            err = st.get("error")
            tok = st.get("token")
            if isinstance(tok, str) and tok:
                if _shape_ok(tok):
                    return tok, "", ""
                return None, f"shape mismatch len={len(tok)}", "shape"
            if err:
                code = str(err).strip()
                if code in WIDGET_FATAL_CODES:
                    return None, f"widget-error: code={code}", "widget-error"
        # Walk-mode HTML regex fallback: the page's own JS may write the
        # token directly into a script tag without populating the input.
        try:
            html = await page.content()
            m = TOKEN_FALLBACK_RE.search(html)
            if m and _shape_ok(m.group(0)):
                return m.group(0), "", ""
        except Exception:
            pass
        elapsed = time.time() - start
        if clicks < len(CLICK_AT_S) and elapsed >= CLICK_AT_S[clicks]:
            clicks += 1
            await _click_widget(page)
        await asyncio.sleep(POLL_MS / 1000)

    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass
    diag = await _frame_diag(page)
    if diag or clicks:
        return None, diag or f"interactive challenge ran ({clicks} click(s)), no mint", "interactive-stall"
    return None, diag or "budget exhausted before mint", "transient"


# --------------------------------------------------------------- mint driver
async def _run_mint(kind: str, payload: dict, budget_ms: int):
    """Shared mint driver: doom-cache check, slot acquisition, in-budget
    retry on transient/driver, honest failClass.

    Returns (token|None, diag, fail_class, attempts, fastfailed: bool, sitekey)."""
    from urllib.parse import urlparse

    sitekey = str(payload.get("sitekey") or "")
    if not sitekey:
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
        slot_idx = -1
        page = None
        slot_rec = None
        try:
            slot_idx, slot_rec = await POOL.acquire_slot()
            page = await slot_rec["ctx"].new_page()
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
            # Detect WARP proxy death — flip the flag for future mints.
            if WARP_ENABLED and POOL.warp_ok and ("ECONNREFUSED" in str(e) or "SOCKS" in str(e).upper()):
                await POOL.mark_warp_dead()
                diag = f"driver (warp-down): {type(e).__name__}: {str(e)[:90]}"
                fail_class = "warp-degraded"
        finally:
            try:
                if page is not None:
                    await page.close()
            except Exception:
                pass
            if slot_idx >= 0:
                POOL.release_slot(slot_idx)
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


# --------------------------------------------------------------- endpoints
@asynccontextmanager
async def lifespan(_app: FastAPI):
    _restore_state()
    if WARMUP:
        POOL.warm_in_background()
    yield
    _persist_state()
    await POOL.shutdown()


app = FastAPI(title="bx-solver", docs_url=None, redoc_url=None, lifespan=lifespan)


async def _browser_state() -> str:
    try:
        for s in POOL._slots:
            if s is not None and s.get("ctx") is not None:
                pages = s["ctx"].pages
                if pages is not None:
                    return "up"
        if POOL.warming or (POOL._warm_task is not None and not POOL._warm_task.done()):
            return "warming"
    except Exception:
        pass
    return "warming" if (POOL.warming or (POOL._warm_task is not None and not POOL._warm_task.done())) else "down"


@app.get("/health")
async def health():
    now = time.time()
    slots_summary = []
    for i, s in enumerate(POOL._slots):
        if s is None:
            slots_summary.append({"slot": i, "state": "unlaunched"})
        else:
            slots_summary.append({
                "slot": i, "busy": s.get("busy", False),
                "mints": s.get("mints", 0), "age_s": int(now - s.get("launched_at", now)),
                "profile": bool(s.get("profile_dir")),
            })
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
        "slots": slots_summary,
        "ctx_pool": sum(1 for s in POOL._slots if s is not None),
        "ctx_pool_size": MAX_CONCURRENT,
        "inflight": _inflight,
        "queued": _queued,
        "doom_after": DOOM_AFTER,
        "doom_ttl_s": DOOM_TTL_S,
        "doomed": {sk: {"failClass": d["failClass"], "expires_in_s": int(d["until"] - now)} for sk, d in _doom.items()},
        "proxy": bool(DEFAULT_PROXY) or WARP_ENABLED,
        "warp": {"enabled": WARP_ENABLED, "ok": POOL.warp_ok, "proxy": WARP_PROXY if WARP_ENABLED else None},
        "persistent_profile": bool(PROFILE_BASE),
        "poll_ms": POLL_MS,
        "click_at_s": CLICK_AT_S,
        "mouse_warmup": MOUSE_WARMUP,
        "state_file": STATE_FILE,
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


@app.get("/doom")
async def doom_get(authorization: str | None = Header(default=None)):
    _check_auth(authorization)
    now = time.time()
    return {
        "doomed": [
            {"key": k, "failClass": v["failClass"], "expires_in_s": int(v["until"] - now)}
            for k, v in _doom.items()
        ],
        "count": len(_doom),
        "ttl_s": DOOM_TTL_S,
        "doom_after": DOOM_AFTER,
    }


@app.delete("/doom")
async def doom_clear(authorization: str | None = Header(default=None)):
    _check_auth(authorization)
    cleared = len(_doom)
    _doom.clear()
    for rec in _sitekeys.values():
        rec["doom_streak"] = 0
    _persist_state()
    _log(f"doom cleared by operator: {cleared} entries")
    return {"ok": True, "cleared": cleared}


@app.get("/metrics")
async def metrics():
    # /metrics intentionally NOT auth-gated so a Prometheus scraper can pull it
    # without a token. The HTTP request still has to reach the service (loopback
    # or via the box's port forward — port 25095 is the only public path).
    now = time.time()
    lines = [
        "# HELP bx_solver_solves_total Total mint attempts by kind and outcome.",
        "# TYPE bx_solver_solves_total counter",
        f'bx_solver_solves_total{{kind="solve",outcome="ok"}} {_stats["solves_ok"]}',
        f'bx_solver_solves_total{{kind="solve",outcome="fail"}} {_stats["solves_fail"]}',
        f'bx_solver_solves_total{{kind="walk",outcome="ok"}} {_stats["walks_ok"]}',
        f'bx_solver_solves_total{{kind="walk",outcome="fail"}} {_stats["walks_fail"]}',
        "# HELP bx_solver_mint_duration_ms Mint duration (rolling p50/p95).",
        "# TYPE bx_solver_mint_duration_ms gauge",
        f"bx_solver_mint_duration_ms{{quantile=\"p50\"}} {_stats['p50_ms']}",
        f"bx_solver_mint_duration_ms{{quantile=\"p95\"}} {_stats['p95_ms']}",
        f"bx_solver_mint_duration_ms{{quantile=\"last\"}} {_stats['last_ms']}",
        "# HELP bx_solver_inflight Current in-flight mints.",
        "# TYPE bx_solver_inflight gauge",
        f"bx_solver_inflight {_inflight}",
        f"bx_solver_queued {_queued}",
        "# HELP bx_solver_rss_mb Tree RSS in MB.",
        "# TYPE bx_solver_rss_mb gauge",
        f"bx_solver_rss_mb {_tree_rss_mb()}",
        f"bx_solver_peak_rss_mb {_stats['peak_rss_mb']}",
        "# HELP bx_solver_doom_count Currently doomed sitekeys.",
        "# TYPE bx_solver_doom_count gauge",
        f"bx_solver_doom_count {len(_doom)}",
        f"bx_solver_retries_total {_stats['retries']}",
        f"bx_solver_busy_429_total {_stats['busy_429']}",
        f"bx_solver_recycles_total {_stats['recycles']}",
        f"bx_solver_fastfails_total {_stats['fastfails']}",
        f"bx_solver_uptime_seconds {int(now - _START)}",
        f"bx_solver_slots_total {MAX_CONCURRENT}",
        f"bx_solver_slots_up {sum(1 for s in POOL._slots if s is not None)}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain")


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
            "version": VERSION,
        }, "", doom_key)
    body = {
        "ok": False,
        "error": diag or "mint failed",
        "failClass": fail_class,
        "advice": ADVICE.get(fail_class, ""),
        "ms": ms,
        "mode": kind,
        "attempts": attempts,
        "version": VERSION,
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


@app.post("/mint")
async def mint(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
):
    """Unified mint endpoint: picks solve vs walk based on inputs.

    mode="auto" (default): if URL is given but no sitekey → walk; else solve.
    mode="solve": force solve (needs sitekey + url).
    mode="walk": force walk (needs url)."""
    _check_auth(authorization)
    mode = str(payload.get("mode") or "auto").lower()
    has_url = bool(str(payload.get("url") or "").strip())
    has_sitekey = bool(str(payload.get("sitekey") or "").strip())
    if mode == "auto":
        mode = "walk" if has_url and not has_sitekey else "solve"
    if mode == "walk":
        return await _mint_endpoint("walk", payload, authorization)
    return await _mint_endpoint("solve", payload, authorization)


@app.get("/")
async def root():
    return {"service": "bx-solver", "version": VERSION, "endpoints": ["/health", "/solve", "/walk", "/mint", "/warm", "/doom", "/metrics"]}


if __name__ == "__main__":
    import uvicorn

    _log(
        f"bx-solver v{VERSION} listening on {HOST}:{PORT} "
        f"(profile={'render-free' if RENDER_FREE else 'standard'}, headless={HEADLESS}, "
        f"brunhild_pin={BRUNHILD_PIN}, max_concurrent={MAX_CONCURRENT}, warmup={WARMUP}, "
        f"queue_wait_s={QUEUE_WAIT_S}, retry={RETRY_ENABLED}, "
        f"recycle_mints={RECYCLE_MINTS or 'off'}, rss_guard_mb={RSS_ABORT_MB or 'off'}, "
        f"persistent_profile={bool(PROFILE_BASE)}, warp={'on' if WARP_ENABLED else 'off'}, "
        f"poll_ms={POLL_MS}, click_at_s={CLICK_AT_S}, mouse_warmup={MOUSE_WARMUP}, "
        f"state_file={STATE_FILE or 'off'}, doom_after={DOOM_AFTER or 'off'} ttl={DOOM_TTL_S}s, "
        f"proxy={'yes' if DEFAULT_PROXY else 'no'})"
    )
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
