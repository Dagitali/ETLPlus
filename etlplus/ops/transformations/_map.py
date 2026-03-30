"""
:mod:`etlplus.ops.transformations._map` module.

Map transformation helpers extracted from :mod:`etlplus.ops.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...utils._types import JSONList
from .._types import MapSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_map',
]


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _apply_map_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Apply a functional map/rename step to a list of records.

    Parameters
    ----------
    records : JSONList
        Input records to transform.
    spec : Any
        Mapping of **old field names** to **new field names**.

    Returns
    -------
    JSONList
        Transformed records.
    """
    if isinstance(spec, Mapping):
        return apply_map(records, spec)

    return records


# SECTION: FUNCTIONS ======================================================== #


def apply_map(
    records: JSONList,
    mapping: MapSpec,
) -> JSONList:
    """
    Map/rename fields in each record.

    Parameters
    ----------
    records : JSONList
        Records to transform.
    mapping : MapSpec
        Mapping of old field names to new field names.

    Returns
    -------
    JSONList
        New records with keys renamed. Unmapped fields are preserved.
    """
    rename_map = dict(mapping)
    result: JSONList = []

    for record in records:
        renamed = {
            new_key: record[old_key]
            for old_key, new_key in rename_map.items()
            if old_key in record
        }
        renamed.update(
            {key: value for key, value in record.items() if key not in rename_map},
        )
        result.append(renamed)

    return result
