#!/bin/bash
set -euo pipefail
ROOT="${SIGNALHUB_BLE_ROOT:-/home/kpi/ble}"
DATA="${SIGNALHUB_DATA_DIR:-$ROOT/data}"
PENDING="$DATA/captures/pending"
ARCHIVE="$DATA/captures/archive"
LOG="$DATA/logs/import.log"
VENV="$ROOT/signalhub/.venv/bin/activate"
SENSOR="${SIGNALHUB_SENSOR_ID:-NRF52840-SNIFFER}"
mkdir -p "$ARCHIVE" "$(dirname "$LOG")"
# shellcheck source=/dev/null
source "$VENV"

stable_path() {
  local f="$1"
  local base
  base="$(basename "$f")"
  echo "$ARCHIVE/$base"
}

while true; do
  shopt -s nullglob
  for f in "$PENDING"/cap-*.pcapng; do
    if [[ -f "${f}.importing" ]]; then
      continue
    fi
    sz1=$(stat -c%s "$f" 2>/dev/null || echo 0)
    sleep "${SIGNALHUB_IMPORT_STABLE_SEC:-3}"
    sz2=$(stat -c%s "$f" 2>/dev/null || echo 0)
    if [[ "$sz1" != "$sz2" ]]; then
      continue
    fi
    touch "${f}.importing"
    dest="$(stable_path "$f")"
    if [[ -e "$dest" ]]; then
      dest="$ARCHIVE/$(date -u +%Y%m%dT%H%M%SZ)-$(basename "$f")"
    fi
    mv "$f" "$dest"
    echo "$(date -Iseconds) import $dest" >>"$LOG"
    if sid="$(signalhub-ble import --pcap "$dest" --sensor "$SENSOR" 2>>"$LOG")"; then
      signalhub-ble summarize --session "$sid" >>"$LOG" 2>&1 || true
      echo "$sid" >"${dest}.session_id"
    else
      echo "$(date -Iseconds) import failed for $dest" >>"$LOG"
    fi
    rm -f "${f}.importing"
  done
  shopt -u nullglob
  sleep "${SIGNALHUB_IMPORT_POLL_SEC:-25}"
done
