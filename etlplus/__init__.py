"""
:mod:`etlplus` package.

Top-level facade for the ETLPlus toolkit.
"""

from .__version__ import __version__
from .config import Config
from .config import load_config

__author__ = 'ETLPlus Team'


# SECTION: EXPORTS ========================================================== #


__all__ = [
    '__author__',
    '__version__',
    'Config',
    'load_config',
]
