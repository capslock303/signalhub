#!/usr/bin/env bash
# Run on the Pi after the project tarball has been extracted under /home/kpi/ble/signalhub.
set -eu
SIGNALHUB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$SIGNALHUB_ROOT"

BLE_ROOT="${SIGNALHUB_BLE_ROOT:-/home/kpi/ble}"
export SIGNALHUB_DATA_DIR="${SIGNALHUB_DATA_DIR:-$BLE_ROOT/data}"
export SIGNALHUB_DB="${SIGNALHUB_DB:-$SIGNALHUB_DATA_DIR/db/signalhub.sqlite}"

if [[ ! -x .venv/bin/python3 ]]; then
  echo "Creating Python venv under $SIGNALHUB_ROOT/.venv"
  python3 -m venv .venv
fi

.venv/bin/pip install -U -q pip wheel
.venv/bin/pip install -q -e .

_ble_src="$SIGNALHUB_ROOT/.venv/bin/signalhub-ble"
_ble_dst=/usr/local/bin/signalhub-ble
if [[ -w /usr/local/bin ]] 2>/dev/null; then
  ln -sf "$_ble_src" "$_ble_dst"
elif sudo -n ln -sf "$_ble_src" "$_ble_dst" 2>/dev/null; then
  true
elif sudo ln -sf "$_ble_src" "$_ble_dst" 2>/dev/null; then
  true
else
  mkdir -p "$BLE_ROOT/bin"
  ln -sf "$_ble_src" "$BLE_ROOT/bin/signalhub-ble"
  echo "NOTE: Could not install into /usr/local/bin (no passwordless sudo, or no TTY for sudo)."
  echo "      CLI symlink: $BLE_ROOT/bin/signalhub-ble"
  echo "      Example: export PATH=\"$BLE_ROOT/bin:\$PATH\""
fi

mkdir -p "$SIGNALHUB_DATA_DIR/exports" "$SIGNALHUB_DATA_DIR/db"

echo "Verifying CLI (report subcommands):"
.venv/bin/signalhub-ble report --help | head -20

if .venv/bin/signalhub-ble report --help | grep -q assess; then
  echo "OK: report assess is available."
else
  echo "WARN: report assess not found in help output."
  exit 1
fi

echo "Deploy_remote finished. Example:"
echo "  cd $BLE_ROOT && signalhub-ble report assess --from 2026-04-01 --to 2026-04-17 --out data/exports/assessment.md"
echo "  (or: $BLE_ROOT/bin/signalhub-ble ... if PATH does not include that bin directory)"
