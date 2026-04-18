#!/usr/bin/env bash
# Deploy signalhub to the Pi from macOS/Linux (or Git Bash on Windows).
set -eu
PI_HOST="${PI_HOST:-192.168.8.112}"
PI_USER="${PI_USER:-kpi}"
BLE_ROOT="${BLE_ROOT:-/home/kpi/ble}"
REMOTE_SHUB="${BLE_ROOT}/signalhub"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIGROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TGZ="/tmp/signalhub-deploy-$$.tgz"

cleanup() { rm -f "$TGZ"; }
trap cleanup EXIT

tar -czf "$TGZ" \
  --exclude=.git --exclude=.venv --exclude=__pycache__ \
  --exclude=.pytest_cache --exclude=.ruff_cache --exclude=.ssh.pi-deploy \
  -C "$SIGROOT" .

echo "Uploading to ${PI_USER}@${PI_HOST}:/tmp/signalhub-deploy.tgz"
scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "$TGZ" "${PI_USER}@${PI_HOST}:/tmp/signalhub-deploy.tgz"

RUNNER_SRC="$SCRIPT_DIR/remote_extract_and_deploy.sh"
scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "$RUNNER_SRC" "${PI_USER}@${PI_HOST}:/tmp/signalhub-remote-extract.sh"

ssh -t -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "${PI_USER}@${PI_HOST}" \
  "/bin/bash /tmp/signalhub-remote-extract.sh '${REMOTE_SHUB}'"

echo "Done."
