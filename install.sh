#!/usr/bin/env bash
# bx-solver — one-shot installer for a fresh Ubuntu 22.04/24.04 box (ARM64/x64).
# Usage:  sudo bash install.sh    (from this directory; copies itself to /opt/bx-solver)
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DST=/opt/bx-solver

[ "$(id -u)" = "0" ] || { echo "run with sudo" >&2; exit 1; }

echo "== apt deps"
apt-get update -qq
apt-get install -y -qq python3-venv python3-pip >/dev/null

echo "== files -> $DST"
mkdir -p "$DST"
install -m 644 "$SRC/bx_solver.py" "$DST/bx_solver.py"
install -m 644 "$SRC/requirements.txt" "$DST/requirements.txt"

id -u bx-solver >/dev/null 2>&1 || useradd --system --home "$DST" --shell /usr/sbin/nologin bx-solver

echo "== venv + patchright"
python3 -m venv "$DST/venv"
"$DST/venv/bin/pip" install -q --upgrade pip
"$DST/venv/bin/pip" install -q -r "$DST/requirements.txt"
# Shared browser cache so the service user (not the installing root) finds the
# patched chromium build.
export PLAYWRIGHT_BROWSERS_PATH="$DST/browsers"
"$DST/venv/bin/python" -m patchright install --with-deps chromium
chown -R bx-solver:bx-solver "$DST"

echo "== /etc/bx-solver.env"
if [ ! -f /etc/bx-solver.env ]; then
  TOKEN="$(openssl rand -hex 24 2>/dev/null || head -c 24 /dev/urandom | xxd -p)"
  cat > /etc/bx-solver.env <<ENV
BX_SOLVER_PORT=8787
BX_HOST=0.0.0.0
# The lane sends "Authorization: Bearer <token>" — set the SAME value in the
# Vercel env as BX_SOLVER_TOKEN.
BX_SOLVER_TOKEN=$TOKEN
# Dual-stack host (Oracle VCN with IPv6): keep 0. IPv4-only host (widget shows
# "Could not load the check"): set 1.
BX_BRUNHILD_PIN=0
BX_MAX_CONCURRENT=2
BX_HEADLESS=1
PLAYWRIGHT_BROWSERS_PATH=$DST/browsers
ENV
  chmod 600 /etc/bx-solver.env
  echo "   generated BX_SOLVER_TOKEN: $TOKEN   (also in /etc/bx-solver.env)"
else
  echo "   keeping existing /etc/bx-solver.env"
  grep -q PLAYWRIGHT_BROWSERS_PATH /etc/bx-solver.env || \
    echo "PLAYWRIGHT_BROWSERS_PATH=$DST/browsers" >> /etc/bx-solver.env
fi

echo "== systemd"
install -m 644 "$SRC/bx-solver.service" /etc/systemd/system/bx-solver.service
systemctl daemon-reload
systemctl enable --now bx-solver
sleep 2
systemctl --no-pager -l status bx-solver | head -8 || true

echo "== self test"
sleep 1
curl -s http://127.0.0.1:8787/health || echo "(health check failed — see journalctl -u bx-solver)"
echo
echo "install done."
