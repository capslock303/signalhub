"""BLE link-layer decrypt: metadata in SQLite + operator guidance (Wireshark/tshark is version-specific)."""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from signalhub.ble.tshark import tshark_path


def upsert_session_crypto_from_observations(conn: sqlite3.Connection, session_id: str) -> None:
    """Count encrypted rows and store/update ble_session_crypto (requires table from init_db)."""
    row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM ble_observations
        WHERE session_id = ? AND encrypted_seen = 1
        """,
        (session_id,),
    ).fetchone()
    n_enc = int(row["n"] or 0) if row else 0
    conn.execute(
        """
        INSERT INTO ble_session_crypto(session_id, encrypted_packets_observed)
        VALUES (?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
          encrypted_packets_observed = excluded.encrypted_packets_observed
        """,
        (session_id, n_enc),
    )
    conn.commit()


def set_session_secrets_path(conn: sqlite3.Connection, session_id: str, secrets_file: Path) -> None:
    p = secrets_file.expanduser().resolve()
    conn.execute(
        """
        INSERT INTO ble_session_crypto(session_id, secrets_file_path, encrypted_packets_observed)
        VALUES (?, ?, COALESCE((SELECT encrypted_packets_observed FROM ble_session_crypto WHERE session_id=?),0))
        ON CONFLICT(session_id) DO UPDATE SET secrets_file_path = excluded.secrets_file_path
        """,
        (session_id, str(p), session_id),
    )
    conn.commit()


def wireshark_decrypt_instructions() -> str:
    return (
        "Wireshark decrypts BLE for **display** using keys you supply (LTK/Link Key/IRK depending on capture).\n"
        "1. Open the PCAP in Wireshark → **Edit → Preferences → Protocols → Bluetooth**.\n"
        "2. Add keys in the **SMP / pairing** or **Bluetooth keys** UAT as appropriate for your capture.\n"
        "3. Confirm **btatt** / **btsmp** payloads decode in the GUI.\n"
        "4. For batch re-export, use **tshark** with the same prefs profile; exact `-o uat:...` strings vary by "
        "Wireshark version — copy the working preference line from the GUI or see the Wireshark BLE docs.\n"
        "\n"
        "Note: many builds apply decrypt only to **dissection**, not to rewriting a decrypted PCAP on disk."
    )


def try_tshark_decrypt_copy(
    pcap_in: Path,
    pcap_out: Path,
    *,
    extra_tshark_args: list[str] | None = None,
) -> tuple[int, str]:
    """Run tshark read/write (no keys). Use extra_tshark_args for your site's working `-o` prefs.

    Returns (exit_code, stderr_tail). Does not guarantee decrypted output — see instructions.
    """
    pcap_in = pcap_in.resolve()
    pcap_out = pcap_out.resolve()
    cmd = [
        tshark_path(),
        "-r",
        str(pcap_in),
        "-w",
        str(pcap_out),
        "-F",
        "pcapng",
    ]
    if extra_tshark_args:
        cmd.extend(extra_tshark_args)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    err = (proc.stderr or "")[-4000:]
    return proc.returncode, err


def suggest_tshark_decrypt_command(pcap_in: Path, pcap_out: Path, secrets_file: Path | None) -> str:
    """Human copy-paste starting point; user must align with their Wireshark version."""
    base = f'"{tshark_path()}" -r "{pcap_in.resolve()}" -w "{pcap_out.resolve()}" -F pcapng'
    if secrets_file:
        base += (
            f'\n# Then add prefs from Wireshark GUI, e.g. keys file: "{secrets_file.resolve()}"'
            f"\n# Example placeholder (often wrong for your build — verify):"
            f'\n# -o bluetooth.enable_decryption:TRUE'
        )
    return base + "\n"
