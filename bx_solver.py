"""
bx-solver — self-hosted Turnstile minting service (patchright + patched chromium).

Why this exists (R&D conclusions of 2026-11-15, see WORKLOG.md):
  * patchright stealth was PROVEN: a neutral Turnstile demo mints in ~1.2s with
    ZERO clicks in headless AND headful mode.
  * Wall #1: brunhild.challenges.cloudflare.com is IPv6-only GLOBALLY — v4-only
    hosts can't even load the challenge. Belt-and-braces fix: the
    --host-resolver-rules pin (BX_BRUNHILD_PIN=1) mapping it to CF's v4 anycast.
    On a dual-stack host (Oracle Always-Free ARM with IPv6 enabled in the VCN)
    leave the pin OFF and the browser uses its native v6.
  * Wall #2: egress-IP scoring. Lootlabs silently rejects mints from poisoned
    IPs. A clean dual-stack host is the fix we can own; this service reports the
    widget's own error text in `diag` so live-fire triage is possible from the
    lane notes.

Two mint modes:
  POST /solve  — generic mint: renders a MINIMAL local widget page at the REAL
                 origin of `url` (the document request is fulfilled locally, the
                 browser URL still shows the real site) with the EXACT
                 sitekey/action/cdata binding. Fast path (~2-8s).
  POST /walk   — real-page walk: loads the actual captcha URL and lets the
                 page's OWN widget mint (binding + hostname honored by
                 construction — same principle as the scrape.do walk leg, but on
                 our own hardware, $0 per mint). Optional auto-click.
  GET  /health — liveness + counters.

Auth: when BX_SOLVER_TOKEN is set every request must send
      "Authorization: Bearer <token>" (401 otherwise).

Env:
  PORT               Render injects this — takes precedence when set
  BX_SOLVER_PORT     default 8787
  BX_SOLVER_TOKEN    shared secret (optional but recommended on a public VPS)
  BX_HOST            bind address, default 0.0.0.0
  BX_MAX_CONCURRENT  parallel mints, default 2 (each context ~60-120MB)
  BX_HEADLESS        1 (default) | 0 (headful — needs Xvfb on servers)
  BX_BRUNHILD_PIN    0 (default) | 1 — pin brunhild.challenges.cloudflare.com
                     to 104.18.17.146 (IPv4-only hosts need this)
  BX_RENDER_FREE     0 (default) | 1 — Render free-tier profile (512MB RAM /
                     0.1 CPU): memory-trimmed chromium, concurrency forced to
                     1, 640x480 viewport, browser torn down after
                     BX_BROWSER_IDLE_KILL_S idle (default 75)
  BX_RSS_ABORT_MB    render-free mint guard: abort the mint (honest fail,
                     lane falls through) if the whole tree exceeds this MB —
                     default 460 when BX_RENDER_FREE=1, 0 (off) otherwise.
                     Tune DOWN if Render logs show OOM restarts, UP if the
                     guard trips on healthy mints (watch /health rss_mb)
Run:  python bx_solver.py   (or the systemd unit, see install.sh / README.md)
"""

import asyncio
import gc
import json
import os
import re
import time

from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

# Render web services must bind $PORT; on a VPS BX_SOLVER_PORT (8787) wins.
PORT = int(os.environ.get("PORT") or os.environ.get("BX_SOLVER_PORT", "8787"))
HOST = os.environ.get("BX_HOST", "0.0.0.0")
AUTH_TOKEN = os.environ.get("BX_SOLVER_TOKEN", "").strip()
MAX_CONCURRENT = max(1, int(os.environ.get("BX_MAX_CONCURRENT", "2")))
HEADLESS = os.environ.get("BX_HEADLESS", "1") != "0"
BRUNHILD_PIN = os.environ.get("BX_BRUNHILD_PIN", "0") == "1"

# Render free-tier profile: 512MB RAM / 0.1 CPU. The physics: the browser is
# the RAM cost, so it must not outlive its usefulness — launch on demand,
# tear down after an idle window, cap the JS heap, one mint at a time.
RENDER_FREE = os.environ.get("BX_RENDER_FREE", "0") == "1"
IDLE_KILL_S = int(os.environ.get("BX_BROWSER_IDLE_KILL_S", "75" if RENDER_FREE else "0"))
if RENDER_FREE:
    MAX_CONCURRENT = 1  # 0.1 CPU + 512MB: strictly one mint at a time
RSS_ABORT_MB = int(os.environ.get("BX_RSS_ABORT_MB", "460" if RENDER_FREE else "0"))

# minted tokens observed in the wild: versioned "1."-prefixed (816 chars seen
# live) and the legacy "0."-prefixed shape — same validation the lane uses.
TOKEN_RE = re.compile(r"^\d\.[A-Za-z0-9_.-]{80,}$")
TOKEN_FALLBACK_RE = re.compile(r"0\.[A-Za-z0-9_-]{100,}")

SOLVE_BUDGET_CAP_MS = 90_000
WALK_BUDGET_CAP_MS = 120_000
CLICK_AT_FRACTIONS = (0.33, 0.62)  # of the budget; proven cadence from the R&D probe

_STARTED = time.time()
_stats = {"solves_ok": 0, "solves_fail": 0, "walks_ok": 0, "walks_fail": 0, "last_ms": 0,
          "peak_rss_mb": 0}


def _tree_rss_mb() -> int:
    """Own + all descendant footprint in MB, measured as PSS (proportional
    share of shared pages — the honest cgroup-style number) with VmRSS as
    fallback. Chromium runs as our child tree, so this is the true footprint
    gauge for a 512MB host (Render free has no shell access — /health is the
    only way to observe it live)."""
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
    """Render-free mint guard, throttled to ~1 check/second. Aborting the
    mint at ~460MB is strictly better than letting the 512MB cgroup OOM-kill
    the service: an honest mint failure lets the lane fall through to the
    paid chain, an OOM restart drops the in-flight lane call."""
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

app = FastAPI(title="bx-solver", docs_url=None, redoc_url=None)


# ---------------------------------------------------------------- browser pool
class BrowserPool:
    """Patched chromium lifecycle. Standard profile: one persistent instance,
    fresh context per mint. Render-free profile: browser reused only within
    the idle window (burst warmth), torn down afterwards — idle footprint
    returns to ~100MB (uvicorn + patchright driver)."""

    def __init__(self) -> None:
        self._pw = None
        self._browser = None
        self._idle_task: asyncio.Task | None = None
        self.lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(MAX_CONCURRENT)

    def _launch_args(self) -> list:
        args = ["--disable-dev-shm-usage", "--no-sandbox"]
        if BRUNHILD_PIN:
            # brunhild.challenges.cloudflare.com is IPv6-only globally; v4-only
            # hosts can't resolve it. CF anycast serves every zone on every
            # edge IP via SNI, so pin the v4-mapped address of its AAAA record.
            args.append(
                "--host-resolver-rules="
                "MAP brunhild.challenges.cloudflare.com 104.18.17.146"
            )
        if RENDER_FREE:
            # 512MB box: cap the JS heap, single renderer, no GPU pipeline.
            args += [
                "--js-flags=--max-old-space-size=128",
                "--renderer-process-limit=1",
                "--disable-gpu",
                "--disable-features=BackForwardCache,Translate",
            ]
        return args

    async def browser(self):
        async with self.lock:
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
            return self._browser

    async def release(self):
        """Render-free profile: re-arm the idle teardown so RAM returns to the
        ~100MB baseline between mints. Standard profile: keep the browser up."""
        if self._idle_task is not None:
            self._idle_task.cancel()
            self._idle_task = None
        if not RENDER_FREE or IDLE_KILL_S <= 0:
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

    async def context(self):
        browser = await self.browser()
        vw, vh = (640, 480) if RENDER_FREE else (1280, 720)
        return await browser.new_context(
            locale="en-US",
            viewport={"width": vw, "height": vh},
        )


POOL = BrowserPool()


def _check_auth(authorization: str | None) -> None:
    if AUTH_TOKEN and authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


def _shape_ok(token: str, sitekey: str = "") -> bool:
    # Cloudflare's DOCUMENTED dummy sitekeys (1x/2x/3x000…) deliberately mint
    # short dummy tokens — they verify against the matching dummy secrets and
    # are meant for exactly this kind of smoke test. Only production keys get
    # the strict shape gate.
    if sitekey[:4] in ("1x00", "2x00", "3x00"):
        return len(token) >= 20
    return bool(TOKEN_RE.match(token)) or bool(TOKEN_FALLBACK_RE.match(token))


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
    while WE control the widget config (exact sitekey/action/cdata binding)."""
    extra = ""
    if action:
        extra += f"        action: {json.dumps(action[:32])},\n"
    if cdata:
        extra += f"        cdata: {json.dumps(cdata[:255])},\n"
    html = (MINT_HTML
            .replace("__SITEKEY__", json.dumps(sitekey))
            .replace("__EXTRA_FIELDS__", extra))

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
        await page.unroute("**/*")

    deadline = time.time() + budget_ms / 1000
    clicks = 0
    while time.time() < deadline:
        guard = _rss_guard_diag()
        if guard:
            return None, guard
        rd = await _reader(page)
        tok = rd.get("tok")
        if isinstance(tok, str) and tok:
            return (tok if _shape_ok(tok, sitekey) else None), f"shape mismatch len={len(tok)}"
        if rd.get("err") and not tok:
            diag = await _frame_diag(page)
            return None, diag or f"error-callback: {rd.get('err')}"
        frac = 1 - (deadline - time.time()) * 1000 / budget_ms
        if clicks < len(CLICK_AT_FRACTIONS) and frac >= CLICK_AT_FRACTIONS[clicks]:
            clicks += 1
            await _click_checkbox(page)
        await asyncio.sleep(0.5)
    return None, await _frame_diag(page) or "budget exhausted before mint"


# --------------------------------------------------------------- page-walk mint
async def mint_walk(page, url: str, budget_ms: int):
    """Load the REAL captcha page and let the page's own widget mint — binding
    and hostname honored by construction. Mirrors the proven scrape.do walk
    (settle waits + click cadence + input reader + HTML regex fallback)."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=min(budget_ms, 45_000))
    except Exception:
        pass  # the page is up enough for the widget in the common case

    deadline = time.time() + budget_ms / 1000
    clicks = 0
    while time.time() < deadline:
        guard = _rss_guard_diag()
        if guard:
            return None, guard
        rd = await _reader(page)
        tok = rd.get("tok")
        if isinstance(tok, str) and tok:
            return (tok if _shape_ok(tok) else None), f"shape mismatch len={len(tok)}"
        try:
            html = await page.content()
            m = TOKEN_FALLBACK_RE.search(html)
            if m and _shape_ok(m.group(0)):
                return m.group(0), ""
        except Exception:
            pass
        frac = 1 - (deadline - time.time()) * 1000 / budget_ms
        if clicks < len(CLICK_AT_FRACTIONS) and frac >= CLICK_AT_FRACTIONS[clicks]:
            clicks += 1
            await _click_checkbox(page)
        await asyncio.sleep(0.5)
    return None, await _frame_diag(page) or "budget exhausted before mint"


# -------------------------------------------------------------------- endpoints
@app.get("/health")
async def health():
    browser = "down"
    try:
        if POOL._browser is not None and POOL._browser.is_connected():
            browser = "up"
    except Exception:
        pass
    return {
        "ok": True,
        "service": "bx-solver",
        "profile": "render-free" if RENDER_FREE else "standard",
        "browser": browser,
        "headless": HEADLESS,
        "brunhild_pin": BRUNHILD_PIN,
        "max_concurrent": MAX_CONCURRENT,
        "rss_mb": _tree_rss_mb(),
        "rss_guard_mb": RSS_ABORT_MB,
        "uptime_s": int(time.time() - _STARTED),
        "stats": _stats,
    }


@app.post("/solve")
async def solve(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
):
    _check_auth(authorization)
    sitekey = str(payload.get("sitekey") or "").strip()
    url = str(payload.get("url") or "").strip() or "https://example.com/"
    if not sitekey:
        return JSONResponse({"ok": False, "error": "sitekey required"}, status_code=400)
    action = str(payload.get("action") or "").strip()
    cdata = str(payload.get("cdata") or "").strip()
    budget = min(int(payload.get("budgetMs") or 45_000), SOLVE_BUDGET_CAP_MS)

    t0 = time.time()
    async with POOL.sem:
        try:
            await POOL.browser()
            _note_rss()
            ctx = await POOL.context()
            try:
                page = await ctx.new_page()
                token, diag = await mint_solve(page, url, sitekey, action, cdata, budget)
            finally:
                await ctx.close()
                _note_rss()
        finally:
            await POOL.release()
            if RENDER_FREE:
                gc.collect()
    ms = int((time.time() - t0) * 1000)
    _stats["last_ms"] = ms
    if token:
        _stats["solves_ok"] += 1
        return {"ok": True, "token": token, "ms": ms, "mode": "solve"}
    _stats["solves_fail"] += 1
    return {"ok": False, "error": diag or "mint failed", "ms": ms, "mode": "solve"}


@app.post("/walk")
async def walk(
    payload: dict = Body(...),
    authorization: str | None = Header(default=None),
):
    _check_auth(authorization)
    url = str(payload.get("url") or "").strip()
    if not url:
        return JSONResponse({"ok": False, "error": "url required"}, status_code=400)
    budget = min(int(payload.get("budgetMs") or 60_000), WALK_BUDGET_CAP_MS)

    t0 = time.time()
    async with POOL.sem:
        try:
            await POOL.browser()
            _note_rss()
            ctx = await POOL.context()
            try:
                page = await ctx.new_page()
                token, diag = await mint_walk(page, url, budget)
            finally:
                await ctx.close()
                _note_rss()
        finally:
            await POOL.release()
            if RENDER_FREE:
                gc.collect()
    ms = int((time.time() - t0) * 1000)
    _stats["last_ms"] = ms
    if token:
        _stats["walks_ok"] += 1
        return {"ok": True, "token": token, "ms": ms, "mode": "walk"}
    _stats["walks_fail"] += 1
    return {"ok": False, "error": diag or "mint failed", "ms": ms, "mode": "walk"}


if __name__ == "__main__":
    import uvicorn

    print(f"bx-solver listening on {HOST}:{PORT} "
          f"(profile={'render-free' if RENDER_FREE else 'standard'}, "
          f"headless={HEADLESS}, brunhild_pin={BRUNHILD_PIN}, "
          f"max_concurrent={MAX_CONCURRENT}, idle_kill_s={IDLE_KILL_S})")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
