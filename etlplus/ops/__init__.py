"""
:mod:`etlplus.ops` package.

ETL / data pipeline operation helpers.


Importing :mod:`etlplus.ops` exposes the handful of coarse-grained helpers most
users care about: ``extract``, ``transform``, ``load``, ``validate``, and
``run``. Each helper delegates to the richer modules under ``etlplus.ops.*``
while presenting a compact public API surface.

Examples
--------
``from etlplus.ops import extract`` is importing the `extract` function from
the :mod:`etlplus.ops.extract` module, making it directly accessible in the
current module without needing to reference the full module path. This allows
you to use the `extract` function as if it were defined in the current module.
from etlplus import extract, transform
>>> raw = extract('file', 'input.json')
>>> curated = transform(raw, {'select': ['id', 'name']})

See Also
--------
- :mod:`etlplus.ops.run` for orchestrating pipeline jobs
"""

from .extract import extract
from .load import load
from .run import run
from .transform import transform
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'extract',
    'load',
    'run',
    'transform',
    'validate',
]
