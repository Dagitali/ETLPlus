"""
:mod:`etlplus.ops.transformations.map` module.

Map and rename helpers shared by :mod:`etlplus.ops.transform` and custom
runners.

Use :func:`apply_map` for direct field renaming. Use :func:`apply_map_step`
when you need the pipeline-style adapter consumed by
:func:`etlplus.ops.transform.transform`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...utils._types import JSONList
from .._types import MapSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'apply_map',
    'apply_map_step',
]


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


def apply_map_step(
    records: JSONList,
    spec: Any,
) -> JSONList:
    """
    Apply a map/rename pipeline step to a list of records.

    Parameters
    ----------
    records : JSONList
        Input records to transform.
    spec : Any
        Mapping of **old field names** to **new field names**.

    Returns
    -------
    JSONList
        Transformed records using the same step semantics as
        :func:`etlplus.ops.transform.transform`.
    """
    if isinstance(spec, Mapping):
        return apply_map(records, spec)

    return records
