"""
:mod:`setup` module.

Compatibility shim for setuptools-based builds.

Project metadata lives in ``pyproject.toml``. This file exists only for tools
that still import or invoke ``setup.py`` directly.
"""

from setuptools import setup  # type: ignore[import-untyped]

# SECTION: SETUP ============================================================ #

setup()
