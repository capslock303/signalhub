#!/bin/bash
# Run on the Raspberry Pi (once): configures packages, venv, sensor row, systemd, udev.
set -euo pipefail
ROOT="${SIGNALHUB_BLE_ROOT:-/home/kpi/ble}"
PROJ="$ROOT/signalhub"
DATA="$ROOT/data"
KPI_USER="${SIGNALHUB_USER:-kpi}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Re-run with sudo: sudo bash $0"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
echo "wireshark-common wireshark-common/install-setuid boolean true" | debconf-set-selections
apt-get update -qq
apt-get install -y -qq \
  python3 python3-venv python3-pip \
  tshark wireshark-common \
  usbutils curl ca-certificates

usermod -aG dialout,wireshark "$KPI_USER" || true

mkdir -p "$DATA"/{captures/pending,captures/archive,db,exports,logs} "$PROJ" "$ROOT/vendor"
chown -R "$KPI_USER:$KPI_USER" "$DATA" "$ROOT/signalhub" "$ROOT/vendor" "$ROOT/tools" 2>/dev/null || true

if [[ ! -x "$PROJ/.venv/bin/python3" ]]; then
  sudo -u "$KPI_USER" python3 -m venv "$PROJ/.venv"
fi
sudo -u "$KPI_USER" "$PROJ/.venv/bin/pip" install -U pip wheel
sudo -u "$KPI_USER" "$PROJ/.venv/bin/pip" install -e "$PROJ"
# Remove legacy PyPI nrfutil if it was ever installed here (pins click 7.x).
sudo -u "$KPI_USER" "$PROJ/.venv/bin/pip" uninstall -y nrfutil 2>/dev/null || true

# Console entrypoint lives in the venv only unless users activate it; put a stable path on PATH.
ln -sf "$PROJ/.venv/bin/signalhub-ble" /usr/local/bin/signalhub-ble

# Nordic **nRF Util** (CLI name: nrfutil): standalone binary from nordicsemi.com — NOT PyPI `nrfutil` 5.x.
# Prefer Linux **aarch64** on Raspberry Pi. If you only have the **x86-64** Linux build, this script enables
# `amd64` multiarch and installs `libc6:amd64` + `libudev1:amd64` so it runs under the existing qemu binfmt.
NRFUTIL_INSTALL_DIR="$ROOT/tools/nrfutil-bin"
sudo -u "$KPI_USER" mkdir -p "$ROOT/tools" "$NRFUTIL_INSTALL_DIR"
NRF_SRC="${SIGNALHUB_NRFUTIL:-$ROOT/vendor/nrfutil}"
if [[ -f "$NRF_SRC" && -s "$NRF_SRC" ]]; then
  install -m 755 "$NRF_SRC" "$NRFUTIL_INSTALL_DIR/nrfutil"
  echo "Installed Nordic nRF Util from $NRF_SRC -> $NRFUTIL_INSTALL_DIR/nrfutil"
elif command -v nrfutil >/dev/null 2>&1; then
  echo "Using nrfutil already on PATH ($(command -v nrfutil))"
else
  echo "WARN: No official nrfutil binary found. Copy an nRF Util Linux executable to:"
  echo "  $ROOT/vendor/nrfutil"
  echo "then re-run this script, or set SIGNALHUB_NRFUTIL to its path. Download: https://www.nordicsemi.com/Products/Development-tools/nRF-Util"
fi
if [[ -x "$NRFUTIL_INSTALL_DIR/nrfutil" ]] && file "$NRFUTIL_INSTALL_DIR/nrfutil" | grep -q "x86-64"; then
  echo "nrfutil is x86-64: enabling amd64 libraries for ARM64 Pi (qemu binfmt)…"
  dpkg --add-architecture amd64 2>/dev/null || true
  apt-get update -qq
  apt-get install -y -qq libc6:amd64 libudev1:amd64 qemu-user-binfmt || true
fi
KPI_HOME="$(getent passwd "$KPI_USER" | cut -d: -f6)"
sudo -u "$KPI_USER" mkdir -p "$KPI_HOME/.local/lib/wireshark/extcap"
if [[ -x "$NRFUTIL_INSTALL_DIR/nrfutil" ]] || command -v nrfutil >/dev/null 2>&1; then
  NRF_PATH="$NRFUTIL_INSTALL_DIR"
  [[ -x "$NRFUTIL_INSTALL_DIR/nrfutil" ]] || NRF_PATH="$(dirname "$(command -v nrfutil)")"
  sudo -u "$KPI_USER" bash -c "
    set -e
    export PATH=\"$NRF_PATH:\$PATH\"
    nrfutil install ble-sniffer completion device
    nrfutil ble-sniffer bootstrap
  " || echo "WARN: nrfutil ble-sniffer install/bootstrap failed (network, permissions, or Wireshark paths)."
fi

SHUB_ENV=(env SIGNALHUB_BLE_ROOT="$ROOT" SIGNALHUB_DATA_DIR="$DATA" SIGNALHUB_DB="$DATA/db/signalhub.sqlite")
sudo -u "$KPI_USER" -H "${SHUB_ENV[@]}" "$PROJ/.venv/bin/signalhub-ble" init-db || true
sudo -u "$KPI_USER" -H "${SHUB_ENV[@]}" "$PROJ/.venv/bin/signalhub-ble" sensor add \
  --id "${SIGNALHUB_SENSOR_ID:-NRF52840-SNIFFER}" \
  --type ble_sniffer \
  --model "nRF Sniffer for Bluetooth LE (1915:522a)" \
  --notes "USB serial host; auto-import on Pi" || true

install -m 644 "$PROJ/scripts/pi/udev/80-signalhub-nrf-ble-sniffer.rules" /etc/udev/rules.d/
install -m 644 "$PROJ/scripts/pi/systemd/signalhub-ble.target" /etc/systemd/system/
install -m 644 "$PROJ/scripts/pi/systemd/signalhub-ble-collector.service" /etc/systemd/system/
install -m 644 "$PROJ/scripts/pi/systemd/signalhub-ble-importer.service" /etc/systemd/system/
install -m 644 "$PROJ/scripts/pi/systemd/signalhub-ble-ledger-refresh.service" /etc/systemd/system/
install -m 644 "$PROJ/scripts/pi/systemd/signalhub-ble-ledger-refresh.timer" /etc/systemd/system/

chmod +x "$PROJ/scripts/pi/"*.sh "$PROJ/scripts/pi/resolve_sniffer_iface.py" 2>/dev/null || true

udevadm control --reload-rules
udevadm trigger || true

systemctl daemon-reload
systemctl enable signalhub-ble-ledger-refresh.timer
systemctl start signalhub-ble-ledger-refresh.timer

shopt -s nullglob
for z in "$ROOT"/vendor/nrf-sniffer*.zip "$ROOT"/vendor/*[Ss]niffer*.zip; do
  echo "Installing Wireshark extcap from $z"
  sudo -u "$KPI_USER" bash "$PROJ/scripts/pi/install_extcap_from_zip.sh" "$z" || true
  break
done
shopt -u nullglob

echo "Install done. Plug the sniffer USB or run: sudo systemctl start signalhub-ble.target"
echo "User $KPI_USER was added to dialout,wireshark — log out and back in for groups to apply."
