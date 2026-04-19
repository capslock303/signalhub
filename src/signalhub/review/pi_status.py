"""Optional SSH health checks against the Raspberry Pi edge (kpi + BLE sniffer)."""

from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SshResult:
    exit_code: int
    stdout: str
    stderr: str


def run_pi_command(
    *,
    host: str,
    user: str,
    remote_command: str,
    identity_file: str | None = None,
    timeout: int = 20,
) -> SshResult:
    """Run a single remote shell command via OpenSSH (non-interactive)."""
    ssh = _find_ssh()
    if not ssh:
        return SshResult(127, "", "ssh executable not found (install OpenSSH Client on Windows).")

    args = [
        ssh,
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        f"{user}@{host}",
        remote_command,
    ]
    if identity_file and str(identity_file).strip():
        idp = Path(identity_file).expanduser()
        if idp.is_file():
            args[1:1] = ["-i", str(idp.resolve())]

    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        return SshResult(proc.returncode, proc.stdout or "", proc.stderr or "")
    except subprocess.TimeoutExpired:
        return SshResult(124, "", f"SSH timed out after {timeout}s")
    except OSError as e:
        return SshResult(1, "", str(e))


def _find_ssh() -> str | None:
    import shutil

    return shutil.which("ssh")


def partition_tshark_for_ble(stdout: str) -> tuple[list[str], list[str]]:
    """Split ``tshark -D`` lines into BLE/sniffer-relevant vs everything else.

    Wireshark always lists all kernel + extcap sources; for this product we
    surface Nordic / serial / Bluetooth stack entries first.
    """
    focus_needles = (
        "nrf",
        "sniffer",
        "nordic",
        "ttyacm",
        "bluetooth0",
        "bluetooth-monitor",
        "extcap",
    )
    focus: list[str] = []
    rest: list[str] = []
    for line in (stdout or "").splitlines():
        low = line.lower()
        if any(n in low for n in focus_needles):
            focus.append(line)
        else:
            rest.append(line)
    return focus, rest


def nrf_sniffer_iface_index(stdout: str) -> str | None:
    """Return the tshark interface number for the Nordic BLE Sniffer line, if any."""
    for line in (stdout or "").splitlines():
        if re.search(r"nrf\s+sniffer|sniffer\s+for\s+bluetooth", line, re.I):
            m = re.match(r"\s*(\d+)\.", line)
            if m:
                return m.group(1)
    return None


def collect_edge_snapshot(
    *,
    host: str,
    user: str,
    identity_file: str | None,
    remote_ble_root: str = "/home/kpi/ble",
) -> dict[str, SshResult]:
    """Predefined read-only diagnostics (no user-controlled remote shell)."""
    qroot = shlex.quote(remote_ble_root)
    cmds = {
        "uptime": "uptime",
        "target": "systemctl is-active signalhub-ble.target 2>/dev/null || echo unknown",
        "collector": "systemctl is-active signalhub-ble-collector.service 2>/dev/null || echo unknown",
        "importer": "systemctl is-active signalhub-ble-importer.service 2>/dev/null || echo unknown",
        "ledger_timer": "systemctl is-active signalhub-ble-ledger-refresh.timer 2>/dev/null || echo unknown",
        "target_status": "systemctl status signalhub-ble.target --no-pager -l 2>&1 | head -n 25",
        "disk": f"df -h {qroot} 2>/dev/null | tail -n 5",
        # Full list: confirms Nordic extcap + serial; index matches optional SIGNALHUB_FORCE_TSHARK_IFACE
        "tshark_ifaces": "tshark -D 2>/dev/null || echo 'tshark not found or no ifaces'",
        "sniffer_usb": (
            "lsusb 2>/dev/null | grep -Ei '1915|nordic|nrf' || "
            "(echo '--- no Nordic VID match; first USB devices:'; lsusb 2>/dev/null | head -n 12)"
        ),
        "sniffer_serial": (
            "ls -l /dev/ttyACM* /dev/ttyUSB* 2>/dev/null || echo '(no /dev/ttyACM* or ttyUSB* — sniffer unplugged?)'"
        ),
        "sniffer_nrfutil": (
            f"N={qroot}/vendor/nrfutil; "
            f"if [ -x \"$N\" ]; then \"$N\" --version 2>&1 | head -n 8; "
            f"else echo \"(no executable at $N — set SIGNALHUB_NRFUTIL or run install_pi.sh)\"; fi"
        ),
        "recent_collector": (
            "journalctl -u signalhub-ble-collector.service -n 22 --no-pager 2>&1 || true"
        ),
        "recent_importer": "journalctl -u signalhub-ble-importer.service -n 15 --no-pager 2>&1 || true",
    }
    out: dict[str, SshResult] = {}
    for key, shell in cmds.items():
        out[key] = run_pi_command(
            host=host,
            user=user,
            identity_file=identity_file,
            remote_command=shell,
            timeout=25 if key in ("recent_importer", "recent_collector") else 18,
        )
    return out
