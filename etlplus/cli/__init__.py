"""
:mod:`etlplus.cli` package.

This package defines the main command-line interface (CLI) command and
subcommands for ``etlplus``.
"""

from __future__ import annotations

from ._main import main

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'main',
]
