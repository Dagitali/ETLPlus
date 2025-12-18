"""
etlplus.__version__ module.

Expose the installed ETLPlus version.
"""

from importlib import metadata as _metadata

try:
    __version__ = _metadata.version('etlplus')
except _metadata.PackageNotFoundError:
    # Local editable installs without metadata fallback to an obvious
    # placeholder.
    __version__ = '0.0.0'
