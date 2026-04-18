#!/bin/bash
# Install Nordic nRF Sniffer for Bluetooth LE Wireshark extcap from the official ZIP.
# Usage: install_extcap_from_zip.sh /path/to/nrf-sniffer-for-bluetooth-le-*.zip
# Download the ZIP from https://www.nordicsemi.com/Products/Development-tools/nRF-Sniffer-for-Bluetooth-LE
set -euo pipefail
ZIP="${1:-${SIGNALHUB_SNIFFER_ZIP:-}}"
if [[ -z "$ZIP" || ! -f "$ZIP" ]]; then
  echo "Usage: $0 /path/to/nrf-sniffer-for-bluetooth-le-....zip" >&2
  echo "Or set SIGNALHUB_SNIFFER_ZIP to that path." >&2
  exit 1
fi

EXTCAP_USER="${SIGNALHUB_EXTCAP_USER:-$HOME}"
DEST="$EXTCAP_USER/.local/lib/wireshark/extcap"
mkdir -p "$DEST"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
unzip -q -o "$ZIP" -d "$TMP"
EXTDIR="$(find "$TMP" -type d -name extcap 2>/dev/null | head -1)"
if [[ -n "$EXTDIR" && -d "$EXTDIR" ]]; then
  cp -a "$EXTDIR/." "$DEST/"
else
  echo "Could not find extcap/ inside ZIP (extracted to $TMP for inspection)." >&2
  find "$TMP" -maxdepth 4 -type d >&2 || true
  exit 1
fi
chmod -R a+rx "$DEST" || true
find "$DEST" -type f \( -name '*.sh' -o -name 'nrf_sniffer_ble*' -o -name '*.py' \) -exec chmod +x {} \; 2>/dev/null || true
echo "Installed extcap into $DEST"
tshark -D 2>/dev/null | grep -iE 'nrf|sniffer' || echo "tshark -D: no nrf line yet (replug USB or restart tshark)."
