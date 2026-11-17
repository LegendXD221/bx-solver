# bx-solver — self-hosted Turnstile minting service

A tiny HTTP service that mints Cloudflare Turnstile tokens with a stealth
browser (patchright + patched chromium) on hardware we own. Built from the
2026-11-15 R&D conclusions: patchright stealth is proven (~1.2s zero-click
mint on a neutral demo, headless AND headful), so the code is light — the
*runtime* is browser-class (~400-600MB RAM). This repo is self-contained and
public on purpose: Render can build and run it straight from this repo on the
free tier (see the `BX_RENDER_FREE=1` profile below), and the same service
runs unchanged on any owned box or VPS.

**The honest 2026-11-17 verdict (measured, see WORKLOG.md):** neutral/dummy
sitekeys mint free forever. PRODUCTION sitekeys refuse self-hosted mints from
a Linux datacenter box regardless of browser build (chromium/real-Chrome),
head mode (headless/headful under Xvfb), or egress IP — unbound challenges die
at the `/fo/` fingerprint submission (HTTP 400), cdata-bound ones run the full
interactive challenge but completion never arrives (`/pat/` answers 401 — a
client-class check that provider fleets satisfy). v3 therefore makes the
service the **fast honest first leg**: every failure is classified
(`failClass`) and doom-cached, so a doomed sitekey costs the calling lane
~0.4s instead of a full 45s budget.

## Endpoints

| Endpoint | Purpose | Typical |
| --- | --- | --- |
| `POST /solve` | Generic mint: minimal widget page rendered **at the real origin** of `url` (document fulfilled locally, browser still talks to the real site) with the exact sitekey/action/cdata binding | ~2-8s |
| `POST /walk` | Real-page walk: loads the actual captcha URL, the page's **own** widget mints (binding + hostname honored by construction — the same principle as the scrape.do walk leg, but on our hardware = $0) | ~5-30s |
| `GET /health` | Liveness + live metrics (p50/p95, inflight/queued, per-sitekey table, doom map, recent diags) | instant |
| `GET`/`POST /warm` | Idempotent browser prelaunch (also runs automatically at boot when `BX_WARMUP=1`) | instant |

Auth: when `BX_SOLVER_TOKEN` is set, send `Authorization: Bearer <token>`.

## v2 hardening (2026-11-17, live on the Altare box)

- **Warm browser at boot** (`BX_WARMUP=1`, standard profile): the first mint
  after any restart no longer pays the ~4s cold start — the browser launches
  in a background task the moment the process comes up (~1.7s) and mints land
  in ~3.5-4s from the internet.
- **Bounded queue** (`BX_QUEUE_WAIT_S`, default 12): a burst no longer parks
  callers on the semaphore until their own abort fires — past the wait the
  endpoint returns an honest `429 {"ok":false,"error":"solver busy …"}` and
  the calling lane falls through to the next provider immediately.
- **One retry within budget** (`BX_RETRY=1`, default): the transient dry round
  (widget loaded, nothing minted before the deadline — observed live on the
  walk leg) re-attempts with the REMAINING budget (attempt 1 gets 65%, the
  retry gets the rest; the response carries `"attempts":2`). Deterministic
  failures (shape mismatch, widget error callback) never retry.
- **Browser recycle** (`BX_BROWSER_RECYCLE_MINTS`, default 40): the chromium
  process relaunches in the background after N mints so RSS stays flat over
  long uptimes — the next mint hits the already-relaunched browser.
- **RSS guard on big boxes** (`BX_RSS_ABORT_MB`, default 4600 non-render-free):
  on a memory-capped cgroup the mint aborts honestly before the OOM-kill
  would drop in-flight lane calls.

```bash
# NEW in v2: response carries attempts; /health carries version + queue + recycle state
# {"ok":true,"token":"1.…","ms":3500,"mode":"solve","attempts":1}
```

```bash
# generic mint with full per-task binding
curl -s http://VPS:8787/solve -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sitekey":"0x4AAAAAAEO6tvECK-X4VCvq","url":"https://…/captcha?urid=…","action":"unlock","cdata":"…"}'
# → {"ok":true,"token":"1.….","ms":4300,"mode":"solve","attempts":1}

# real-page walk (lootlabs captcha URL straight from the lane)
curl -s http://VPS:8787/walk -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"url":"https://…/captcha?urid=…","budgetMs":60000}'
# → {"ok":true,"token":"1.….","ms":25000,"mode":"walk"}   or {"ok":false,"error":"widget: …"}
```

On failure the response carries `error` with the **widget's own error text**
("Could not load the check…", error-callback values), a machine-readable
**`failClass`**, and an **`advice`** string — that is what makes live-fire
triage and instant fall-through possible from the lane's honest-fail notes.

## v3 (2026-11-17): failure classification + doom cache + warm context pool

- **failClass** on every failed mint: `fo-reject` (4xx on the fingerprint
  submission — dead challenge, detected EARLY, the loop aborts in seconds),
  `interactive-stall` (challenge ran, click landed, completion refused — the
  measured client-class wall), `widget-error`, `shape`, `driver`
  (browser/context crashed — retryable), `transient` (dry round — retryable),
  `rss-guard`, and `doom-cached` (see below).
- **Per-sitekey doom cache** (`BX_DOOM_AFTER=2`, `BX_DOOM_TTL_S=1800`): after
  2 consecutive deterministic failures for a sitekey, mints for it answer in
  **<0.5s** with `failClass:"doom-cached"` until the TTL elapses — then ONE
  full-budget re-probe keeps the system adaptive if Cloudflare ever relaxes
  the wall. Walk-mode dooms are keyed by target host (walks carry no
  sitekey); solve-mode by sitekey. `/health` exposes the live doom map.
- **Warm context pool** (`BX_POOL_SIZE`, default = max_concurrent): contexts
  are pre-created in the background and a mint borrows one — the ~0.5-1s
  per-mint context setup leaves the critical path. Contexts stay single-use
  (fingerprint hygiene); the pool refills itself.
- **Live metrics**: `/health` carries p50/p95 over the last 20 successful
  mints, inflight/queued counts, a per-sitekey table (ok/fail/classes/
  fastfails/p50/p95), browser age, and the last 20 diag events.
- **Retry hardening**: `driver`-class failures (browser died mid-mint, e.g.
  during a background recycle) retry on a fresh context within the remaining
  budget; transient dry rounds keep the v2 65/35 budget split; classified
  deterministic failures never retry.
- **Optional proxy passthrough** (`BX_PROXY="http://host:port|user|pass"` env
  or per-request `"proxy": {...}`): routes the mint context through an HTTP
  proxy. EXPERIMENTAL — measured 2026-11-17 to NOT unlock the LootLabs wall
  (challenge assets misbehave through the MITM tunnel); kept for
  lenient-sitekey scenarios where egress reputation matters.
- **`BX_BRUNHILD_PIN` is obsolete**: brunhild.challenges.cloudflare.com left
  DNS entirely (2026-11-17, both A and AAAA empty) and the real widget never
  contacts it — everything rides challenges.cloudflare.com (dual-stack). The
  env knob is kept only so old env files don't break; the MAP rule is inert.

## Deploy (any always-on box — Oracle NOT required)

The service runs anywhere patchright runs (Ubuntu/Debian via `install.sh`,
or plain `pip install -r requirements.txt && python -m patchright install
chromium && python bx_solver.py` on Windows/mac). What actually matters for
Turnstile is the **egress**, not the cloud brand:

- **Wall #2 — IP reputation**: residential > datacenter. A home connection
  is the best free substrate that exists. (Measured 2026-11-17: even a clean
  datacenter egress cannot complete PRODUCTION sitekeys — the client-class
  check refuses Linux-datacenter completions regardless of IP; neutral
  sitekeys mint fine everywhere.)
- **Wall #1 — retired**: brunhild.challenges.cloudflare.com left DNS entirely
  (2026-11-17) and the widget never contacts it — everything rides
  challenges.cloudflare.com, which is dual-stack and reachable from any host.

**Option 1 — a machine you own (recommended: residential IP, $0):**

```bash
sudo bash install.sh                  # Linux; BX_SOLVER_TOKEN auto-generated
# Expose it WITHOUT port-forwarding via a Cloudflare tunnel (free):
cloudflared tunnel --url http://127.0.0.1:8787      # quick URL (testing)
# or a named tunnel on your own domain for a stable URL
```

**Option 2 — cheap VPS ($3-6/mo):** same `install.sh`. Datacenter-IP
reputation is the same gamble every remote solver takes — and for production
sitekeys v3's doom cache makes the failure cheap and honest (fast-fail +
failClass instead of a silent 45s burn).

**Option 3 — Oracle Always-Free ARM:** also works (dual-stack, $0) —
`install.sh` was written for Ubuntu ARM64 as-is. Requires enabling IPv6 in
the VCN (VCN → add IPv6 CIDR → subnet → VNIC assign v6) or the pin.

Smoke test from the box:
```bash
curl -s http://127.0.0.1:8787/health
curl -s http://127.0.0.1:8787/solve -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sitekey":"1x00000000000000000000AA","url":"https://example.com/"}'
# sitekey above = Cloudflare's documented always-pass TEST key (safe proof)
```

## Render free tier (512MB RAM / 0.1 CPU) — the BX_RENDER_FREE profile

Render's free web service is 512MB RAM / 0.1 CPU, spins down after 15 min
idle, and grants **750 free instance hours per workspace per month** (when
the pool drains, Render suspends ALL free services of that workspace).
Deploying on a **separate Render account/workspace** gives the solver its
own pool: 24/7 warm ≈ 720-744h ≤ 750h — it fits, and it never competes with
the bot's workspace (the bot, kept warm by keepalive, already eats ~720h of
its own account's 750h — a second warm service THERE would suspend the bot
around day 21 of the month. Don't co-locate them.)

### LIVE-FIRE VERDICT (2026-11-16, deployed on the second account)

The service deploys cleanly and every safety mechanism works on Render
free — but **the mint itself does not fit in 512MB**. Measured live:

| Gauge | Value |
| --- | --- |
| baseline, browser down (`/health`) | **48MB** (python + uvicorn) |
| baseline, driver spawned, browser down | **166MB** (python + patchright node driver — matches the lab floor) |
| mint peak, cold `/solve` | **534MB** → rss-guard honest-abort at 460MB, 18.5s |
| mint peak, warm `/solve` | **537MB** → same honest abort, 7.8s |
| squeeze (heap 128, viewport 640x480, BackForwardCache/Translate off) | **−0 effect** — cold 539MB / warm 537MB |
| idle-kill after mints | verified live: 435 → 166MB, browser `down` |

Why the squeeze fails: the weight is **not the V8 heap** — it's chromium's
rendering arenas plus **Turnstile's WASM module** (linear memory sits
outside `--max-old-space-size`). Heap caps can't reach it, and the only
bigger lever (headless-shell instead of full chromium) is a stealth trade
this repo deliberately refuses (Wall #2 odds first).

**What this means operationally:**

- Render free = safe but **no completed mints**: every call honest-fails in
  ~8-19s and the lane falls through to the paid chain. Keep
  `BX_SOLVER_URL` **unset** in Vercel — wiring it would add ~18s of wasted
  latency to every lootlabs solve. The deployed service costs $0 and works
  as a live canary (gauges, guard, lifecycle all proven).
- The real fix is **RAM, not tuning**: run the solver on a substrate with
  ≥1GB — a machine you own (free, residential egress = best Wall #2 odds,
  `install.sh`) or any ≥1GB VPS/box. The same repo deploys unchanged.
  On Render itself that means flipping this service to a bigger instance
  type on the solver account (same repo, one dropdown — the render-free
  profile only activates the safety rails, it never hurts a bigger box).
- If you keep the free service as a canary: expect the honest-fail guard
  every time (`ok:false, "rss-guard: …"`) — that is the system working,
  not an outage.

`BX_RENDER_FREE=1` shrinks the runtime to fit:

| Mechanism | Effect |
| --- | --- |
| concurrency forced to 1 | 0.1 CPU + 512MB: one mint at a time |
| browser torn down after 75s idle (`BX_BROWSER_IDLE_KILL_S`) | baseline drops to ~165MB (python + patchright driver); the browser exists only during mints |
| 640x480 viewport, `--js-flags=--max-old-space-size=128`, `--renderer-process-limit=1`, `--disable-gpu`, BackForwardCache/Translate off | lighter renderers (note: live-fire showed the mint still exceeds 512MB — see the verdict above) |
| **RSS mint guard** (`BX_RSS_ABORT_MB`, default 460) | aborts the mint honestly (`"error":"rss-guard: tree hit …MB"`) BEFORE the 512MB cgroup OOM-kills the service — an honest fail lets the lane fall through to the paid chain; an OOM restart would drop the in-flight lane call instead |
| `/health` gauges | `rss_mb` (whole-tree PSS — the cgroup-style number), `stats.peak_rss_mb`, `rss_guard_mb` — Render free has no shell, these are your eyes |
| `PORT` env precedence | binds what Render injects |

Deploy click-path (Render dashboard):

1. **New → Web Service** → connect **this repo** (`LegendXD221/bx-solver`,
   public — Render builds it straight from the repo)
2. **Runtime** `Docker` (image carries the patched chromium; the repo root
   is the service root — no Root Directory needed). Lighter-build
   alternative — Runtime `Python`:
   Build `pip install -r requirements.txt && python -m patchright install --with-deps chromium`,
   Start `python bx_solver.py`
3. **Instance Type** Free — **Health Check Path** `/health`
4. Environment: `BX_RENDER_FREE=1`, `BX_BRUNHILD_PIN=1` (Render egress is
   IPv4-only — the pin is mandatory there), `BX_SOLVER_TOKEN=<secret>`
5. Keep-warm: an external `/health` ping every 10 min prevents spin-down
   (a cold boot is 30-90s — the lane's 60s budget can't wait for it).
   cron-job.org or UptimeRobot (free, no repo coupling) — or a GitHub
   Actions cron in any public repo, same pattern as the bot's keepalive.
6. Wire the lane (Vercel env): `BX_SOLVER_URL=https://<service>.onrender.com`
   + `BX_SOLVER_TOKEN` → LEG -1 + the selfsolver provider activate on the
   next deploy. Kill switch unchanged: `BX_SOLVER_URL=off`.

First-fire checklist (RE-ANSWERED 2026-11-16 on the deployed free service —
kept here for the next substrate; see the verdict above for the free-tier
answers):

```bash
curl -s https://<service>.onrender.com/health        # rss_mb baseline ~165
curl -s https://<service>.onrender.com/solve -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sitekey":"1x00000000000000000000AA","url":"https://example.com/"}'
# then a REAL lootlabs /walk — this is the Wall #2 verdict:
curl -s https://<service>.onrender.com/walk -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"url":"<capUrl>","budgetMs":60000}'
# watch: stats.peak_rss_mb, rss-guard events, ms vs the 60s lane budget
```

- **CPU 0.1**: mints run 5-15× slower than the 1-3s sandbox numbers; whether
  a real /walk fits the 60s lane budget is a live measurement (`ms` in the
  response tells you immediately).
- **Wall #2**: Render's shared datacenter egress IPs are the same gamble
  every remote solver takes; the widget's own error text in `diag` gives the
  silent-reject verdict on the first live mint. (Never reached on free —
  the RSS wall fires first.)
- If you see OOM restarts in Render logs → lower `BX_RSS_ABORT_MB` (460→420);
  if the guard trips on healthy mints → raise it (460→490). `/health`
  `stats.peak_rss_mb` is the tuning oracle. On ≥1GB substrates set
  `BX_RENDER_FREE=0` (or unset it) — the guard stays off and concurrency
  returns to normal.

## Wire into the lane (Vercel env, zero code changes needed)

```
BX_SOLVER_URL    = http://VPS_IP:8787     (or https://solver.example.com)
BX_SOLVER_TOKEN  = <same value as /etc/bx-solver.env>
```

Activation is env-only and takes effect on the next deploy. Once set:

- **LootLabs lane, new LEG -1**: before the paid scrape.do walk, the lane calls
  `/walk` on this service — a PASS costs **$0** instead of ~25-50 scrape.do
  credits. Rejection/timeout falls through to the scrape.do walk and then the
  provider chain, unchanged.
- **Shared Turnstile chain**: a `bx-solver` provider is prepended to
  `solveTurnstileToken` (benefits Delta and every other Turnstile gate). It
  carries cdata by construction, so it keeps its first place even in the
  binding-first order.
- Kill switch: `BX_SOLVER_URL=off` (same convention as `BYPASSLAT_API_KEY=off`).

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `widget: Could not load the check. Please reload.` | v4-only host can't resolve the IPv6-only challenge host | enable IPv6 in the VCN (preferred) or set `BX_BRUNHILD_PIN=1` |
| Mint times out silently, challenge loads, no callback (beacons 204, no error) | egress-IP scoring rejected the mint (Wall #2) — datacenter IP poisoned for this sitekey | nothing code-side; walk mode from a clean dual-stack IP is the best shot, and the lane falls through to the paid chain |
| `MemoryMax` kills the unit | runaway contexts | lower `BX_MAX_CONCURRENT` to 1 |
| `systemctl status` shows browser launch failure | missing system libs | rerun `install.sh` (it runs `patchright install --with-deps chromium`) |

## What "lightweight" means here (honest scope)

- **Code**: one ~400-line Python file + a systemd unit. No queue, no DB, no
  framework beyond FastAPI.
- **Runtime**: browser-class physics — a real (patched) Chromium, ~400-600MB
  RSS on the standard profile. The `BX_RENDER_FREE=1` profile squeezes the
  same proven machinery into Render free's 512MB (see the section above);
  every maintained no-browser Turnstile approach died in the 2026 sweep.
- **Cost**: $0/mo on Oracle free ARM, Render free (own workspace) or a home
  machine vs NopeCHA Starter $4.99/mo vs scrape.do ~25-50 credits per
  lootlabs solve.
