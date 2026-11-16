# bx-solver — self-hosted Turnstile minting service

A tiny HTTP service that mints Cloudflare Turnstile tokens with a stealth
browser (patchright + patched chromium) on hardware we own. Built from the
2026-11-15 R&D conclusions: patchright stealth is proven (~1.2s zero-click
mint on a neutral demo, headless AND headful), so the code is light — the
*runtime* is browser-class (~400-600MB RAM) and on an IPv4-only host the
IPv6-only challenge host needs `BX_BRUNHILD_PIN=1`. This repo is
self-contained and public on purpose: Render can build and run it straight
from this repo on the free tier (see the `BX_RENDER_FREE=1` profile below),
and the same service runs unchanged on any owned box or VPS.

## Endpoints

| Endpoint | Purpose | Typical |
| --- | --- | --- |
| `POST /solve` | Generic mint: minimal widget page rendered **at the real origin** of `url` (document fulfilled locally, browser still talks to the real site) with the exact sitekey/action/cdata binding | ~2-8s |
| `POST /walk` | Real-page walk: loads the actual captcha URL, the page's **own** widget mints (binding + hostname honored by construction — the same principle as the scrape.do walk leg, but on our hardware = $0) | ~5-30s |
| `GET /health` | Liveness + counters | instant |

Auth: when `BX_SOLVER_TOKEN` is set, send `Authorization: Bearer <token>`.

```bash
# generic mint with full per-task binding
curl -s http://VPS:8787/solve -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sitekey":"0x4AAAAAAEO6tvECK-X4VCvq","url":"https://…/captcha?urid=…","action":"unlock","cdata":"…"}'
# → {"ok":true,"token":"1.….","ms":4300,"mode":"solve"}

# real-page walk (lootlabs captcha URL straight from the lane)
curl -s http://VPS:8787/walk -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"url":"https://…/captcha?urid=…","budgetMs":60000}'
# → {"ok":true,"token":"1.….","ms":25000,"mode":"walk"}   or {"ok":false,"error":"widget: …"}
```

On failure the response carries `error` with the **widget's own error text**
("Could not load the check…", error-callback values) — that is what makes
live-fire triage possible from the lane's honest-fail notes.

## Deploy (any always-on box — Oracle NOT required)

The service runs anywhere patchright runs (Ubuntu/Debian via `install.sh`,
or plain `pip install -r requirements.txt && python -m patchright install
chromium && python bx_solver.py` on Windows/mac). What actually matters for
Turnstile is the **egress**, not the cloud brand:

- **Wall #2 — IP reputation**: residential > datacenter. A home connection
  is the best free substrate that exists.
- **Wall #1 — the IPv6-only challenge host**: native dual-stack is ideal;
  on an IPv4-only host just set `BX_BRUNHILD_PIN=1` (proven transport fix —
  the pin ships in the service, default 0).

**Option 1 — a machine you own (recommended: residential IP, $0):**

```bash
sudo bash install.sh                  # Linux; BX_SOLVER_TOKEN auto-generated
# Expose it WITHOUT port-forwarding via a Cloudflare tunnel (free):
cloudflared tunnel --url http://127.0.0.1:8787      # quick URL (testing)
# or a named tunnel on your own domain for a stable URL
```

**Option 2 — cheap VPS ($3-6/mo):** same `install.sh`; add
`BX_BRUNHILD_PIN=1` if the box is IPv4-only. Datacenter-IP reputation is the
same gamble every remote solver takes.

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

`BX_RENDER_FREE=1` shrinks the runtime to fit:

| Mechanism | Effect |
| --- | --- |
| concurrency forced to 1 | 0.1 CPU + 512MB: one mint at a time |
| browser torn down after 75s idle (`BX_BROWSER_IDLE_KILL_S`) | baseline drops to ~165MB (python + patchright driver); the browser exists only during mints |
| 800x600 viewport, `--js-flags=--max-old-space-size=192`, `--renderer-process-limit=1`, `--disable-gpu` | lighter renderers |
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

First-fire checklist (the two live unknowns the sandbox cannot answer):

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
  silent-reject verdict on the first live mint.
- If you see OOM restarts in Render logs → lower `BX_RSS_ABORT_MB` (460→420);
  if the guard trips on healthy mints → raise it (460→490). `/health`
  `stats.peak_rss_mb` is the tuning oracle.

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
