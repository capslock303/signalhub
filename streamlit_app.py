"""Streamlit entrypoint for Community Cloud and local runs.

In Streamlit Cloud → App settings → Main file path, set:
  streamlit_app.py
(repository root, this file).

Do not rely on ``if __name__ == "__main__"`` inside the library module; Cloud may not
invoke it when the main file path points deep under ``src/``.
"""

from __future__ import annotations

from signalhub.review.app import main

main()
