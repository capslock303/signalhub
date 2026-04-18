#!/usr/bin/env python3
"""Pick tshark/dumpcap interface index for Nordic nRF BLE Sniffer (extcap)."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys


def main() -> int:
    forced = (os.environ.get("SIGNALHUB_FORCE_TSHARK_IFACE") or "").strip()
    if forced.isdigit():
        print(forced)
        return 0

    tshark = shutil.which("tshark")
    if not tshark:
        print("tshark not found", file=sys.stderr)
        return 1
    try:
        out = subprocess.check_output([tshark, "-D"], text=True, stderr=subprocess.STDOUT, timeout=30)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(str(e), file=sys.stderr)
        return 1
    for line in out.splitlines():
        if re.search(r"nrf|sniffer|bluetooth\s*le|bt\s*le", line, re.I):
            m = re.match(r"^\s*(\d+)\.", line)
            if m:
                print(m.group(1))
                return 0
    print(out, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
