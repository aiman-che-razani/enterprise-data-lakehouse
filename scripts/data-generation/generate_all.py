#!/usr/bin/env python3
"""
Thin wrapper around `python -m datasets.generators`.

Usage:
    python scripts/data-generation/generate_all.py [args...]

All arguments are forwarded to the generator CLI. Run with --help for options.
"""
from __future__ import annotations

import subprocess
import sys


def main() -> None:
    cmd = [sys.executable, "-m", "datasets.generators", *sys.argv[1:]]
    result = subprocess.run(cmd, check=False)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
