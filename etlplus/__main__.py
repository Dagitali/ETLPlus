"""
etlplus.__main__

Thin wrapper so `python -m etlplus` invokes the same CLI entrypoint as the
console script defined in `pyproject.toml`.
"""
from __future__ import annotations

import sys

from .cli import main


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':
    sys.exit(main())
