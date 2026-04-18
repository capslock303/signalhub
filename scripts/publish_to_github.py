#!/usr/bin/env python3
"""Create the GitHub repo under capslock303 (if it does not exist) via the REST API.

Does not push code — run git push yourself so credentials stay in Git Credential Manager / SSH.

Usage (PowerShell, token must NOT be committed):

  $env:GITHUB_TOKEN = 'ghp_....'   # classic PAT with repo scope, or fine-grained with repo create
  python scripts/publish_to_github.py

Or load from a local file you never commit:

  Get-Content ..\\.env | ForEach-Object { if ($_ -match '^GITHUB_TOKEN=(.+)$') { $env:GITHUB_TOKEN = $matches[1].Trim() } }
  python scripts/publish_to_github.py

Security: never paste tokens into the repo or Streamlit Secrets as GITHUB_TOKEN for public apps;
Streamlit Community Cloud links to GitHub in the browser and does not need this token.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

OWNER = "capslock303"
REPO = "signalhub"
DESCRIPTION = "BLE captures, SQLite ledger, Streamlit dashboard — Nordic nRF52840 DK / tshark."


def _api(method: str, url: str, token: str, payload: dict | None = None) -> tuple[int, str]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def main() -> int:
    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if not token:
        print(
            "Set GITHUB_TOKEN or GH_TOKEN in the environment (see script docstring).",
            file=sys.stderr,
        )
        return 1

    get_url = f"https://api.github.com/repos/{OWNER}/{REPO}"
    code, body = _api("GET", get_url, token)
    if code == 200:
        print(f"Repository already exists: https://github.com/{OWNER}/{REPO}")
    elif code == 404:
        post_url = "https://api.github.com/user/repos"
        code2, body2 = _api(
            "POST",
            post_url,
            token,
            {
                "name": REPO,
                "description": DESCRIPTION,
                "private": False,
                "auto_init": False,
            },
        )
        if code2 not in (200, 201):
            print(f"Create repo failed HTTP {code2}:\n{body2}", file=sys.stderr)
            return 1
        print(f"Created https://github.com/{OWNER}/{REPO}")
    else:
        print(f"Unexpected GET {get_url} → HTTP {code}:\n{body}", file=sys.stderr)
        return 1

    print("\nFrom this folder (after `git add` / `git commit`):")
    print(f"  git remote add origin https://github.com/{OWNER}/{REPO}.git")
    print("  git branch -M main")
    print("  git push -u origin main")
    print("\nIf `remote origin` already exists, use `git remote set-url origin ...` instead.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
