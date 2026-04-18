#!/bin/bash
set -euo pipefail
ROOT="${SIGNALHUB_BLE_ROOT:-/home/kpi/ble}"
DATA="${SIGNALHUB_DATA_DIR:-$ROOT/data}"
PENDING="$DATA/captures/pending"
LOG="$DATA/logs/capture.log"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$PENDING" "$(dirname "$LOG")"
IFACE_CMD=(python3 "$SCRIPT_DIR/resolve_sniffer_iface.py")
LAST_WAIT_LOG=0

while true; do
  if ! IFACE="$("${IFACE_CMD[@]}" 2>/dev/null)"; then
    now=$(date +%s)
    if (( now - LAST_WAIT_LOG >= 60 )); then
      echo "$(date -Iseconds) waiting for sniffer interface (install Nordic extcap ZIP; see README)" >>"$LOG"
      LAST_WAIT_LOG=$now
    fi
    sleep "${SIGNALHUB_CAPTURE_RETRY_SEC:-15}"
    continue
  fi
  OUT="$PENDING/cap-$(date -u +%Y%m%dT%H%M%SZ)-$$.pcapng"
  echo "$(date -Iseconds) tshark -i $IFACE -> $OUT" >>"$LOG"
  # Fixed-duration slices so files are complete for the importer.
  if ! tshark -i "$IFACE" -a "duration:${SIGNALHUB_SLICE_SEC:-300}" -w "$OUT" -q >>"$LOG" 2>&1; then
    echo "$(date -Iseconds) tshark exited non-zero" >>"$LOG"
    sleep 5
  fi
done
