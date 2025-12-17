"""
:mod:`etlplus` package.

Top-level facade for the ETLPlus toolkit.

Importing :mod:`etlplus` exposes the handful of coarse-grained helpers most
users care about: ``extract``, ``transform``, ``load``, ``validate``, and
``run``. Each helper delegates to the richer modules under ``etlplus.*`` while
presenting a compact public API surface.

Examples
--------
>>> from etlplus import extract, transform
>>> raw = extract('file', 'input.json')
>>> curated = transform(raw, {'select': ['id', 'name']})

See Also
--------
- :mod:`etlplus.cli` for the command-line interface
- :mod:`etlplus.run` for orchestrating pipeline jobs
"""

from .__version__ import __version__

__author__ = 'ETLPlus Team'

from .extract import extract
from .load import load
from .run import run
from .transform import transform
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    '__version__',
    'extract',
    'load',
    'run',
    'transform',
    'validate',
]
