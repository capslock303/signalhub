from __future__ import annotations

import csv
import io
import logging
import subprocess
from pathlib import Path
from typing import Iterator

from signalhub.config import tshark_field_list, tshark_path

logger = logging.getLogger(__name__)

# Wireshark field names vary slightly by version; override via SIGNALHUB_TSHARK_FIELDS.
# Wireshark 4.6+ uses btcommon.eir_ad.* for AD parsing; older releases used btcommon.eir.*.
# Omit fields that are not registered in all versions (avoids tshark "Some fields aren't valid").
DEFAULT_FIELDS: tuple[str, ...] = (
    "frame.number",
    "frame.time_epoch",
    "frame.protocols",
    "btle.advertising_address",
    "btle.scanning_address",
    "btle.advertising_header.pdu_type",
    "nordic_ble.rssi",
    "btcommon.eir_ad.entry.device_name",
    "btcommon.eir_ad.entry.company_id",
    "btcommon.eir_ad.entry.uuid_16",
)


def resolve_fields() -> list[str]:
    override = tshark_field_list()
    if override:
        return override
    return list(DEFAULT_FIELDS)


def iter_field_rows(pcap: Path) -> Iterator[dict[str, str]]:
    """Run tshark -T fields and yield one dict per frame (string values only)."""
    fields = resolve_fields()
    cmd = [
        tshark_path(),
        "-r",
        str(pcap.resolve()),
        "-T",
        "fields",
        "-E",
        "header=y",
        "-E",
        "separator=\t",
        "-E",
        "quote=n",
        "-E",
        "occurrence=f",
    ]
    for f in fields:
        cmd.extend(["-e", f])

    logger.info("Running tshark (%d fields) on %s", len(fields), pcap)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or "").strip()
        raise RuntimeError(
            f"tshark failed (exit {proc.returncode}).\n{err}\n"
            "Install Wireshark so tshark is available, set SIGNALHUB_TSHARK, "
            "or set SIGNALHUB_TSHARK_FIELDS to a comma-separated list valid for your version."
        )

    buf = io.StringIO(proc.stdout)
    reader = csv.DictReader(buf, delimiter="\t")
    if reader.fieldnames is None:
        return
    for row in reader:
        out: dict[str, str] = {}
        for k, v in row.items():
            if k is None:
                continue
            out[k.strip()] = (v or "").strip()
        yield out
