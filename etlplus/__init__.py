"""
:mod:`etlplus` package.

Top-level facade for the ETLPlus toolkit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .__version__ import __version__

if TYPE_CHECKING:
    from .config import Config

__author__ = 'ETLPlus Team'


# SECTION: EXPORTS ========================================================== #


__all__ = [
    '__author__',
    '__version__',
    'Config',
]


# SECTION: FUNCTIONS ======================================================== #


def __getattr__(
    name: str,
) -> object:
    """
    Lazily resolve heavyweight top-level exports.
    """
    if name == 'Config':
        from .config import Config

        return Config
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
