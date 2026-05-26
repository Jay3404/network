#!/usr/bin/env python3
"""Compatibility entry point for the week-1 Azure trace script."""

from __future__ import annotations

import runpy
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "code" / "azure_trace_week1.py"


if __name__ == "__main__":
    runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
