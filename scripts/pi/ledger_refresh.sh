#!/bin/bash
set -euo pipefail
ROOT="${SIGNALHUB_BLE_ROOT:-/home/kpi/ble}"
VENV="$ROOT/signalhub/.venv/bin/activate"
LOG="${SIGNALHUB_DATA_DIR:-$ROOT/data}/logs/ledger.log"
mkdir -p "$(dirname "$LOG")"
# shellcheck source=/dev/null
source "$VENV"
{
  echo "$(date -Iseconds) ledger rebuild"
  signalhub-ble ledger rebuild
  echo "$(date -Iseconds) classify"
  signalhub-ble classify
} >>"$LOG" 2>&1
