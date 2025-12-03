"""
:mod:`etlplus.api.utils` module.

Small shared helpers for :mod:`etlplus.api` modules.
"""
from __future__ import annotations

from etlplus.utils import to_float
from etlplus.utils import to_int
from etlplus.utils import to_maximum_float
from etlplus.utils import to_maximum_int
from etlplus.utils import to_minimum_float
from etlplus.utils import to_minimum_int
from etlplus.utils import to_positive_float
from etlplus.utils import to_positive_int


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Float coercion
    'to_float',
    'to_maximum_float',
    'to_minimum_float',
    'to_positive_float',

    # Integer coercion
    'to_int',
    'to_maximum_int',
    'to_minimum_int',
    'to_positive_int',
]
