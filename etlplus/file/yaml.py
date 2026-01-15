"""
:mod:`etlplus.file.yaml` module.

Helpers for reading/writing YAML files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..utils import count_records

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


# TODO: Replace with get_module.
def _get_yaml() -> Any:
    """
    Return the PyYAML module, importing it on first use.

    Raises an informative ImportError if the optional dependency is missing.
    """
    from ._imports import get_optional_module

    return get_optional_module(
        'yaml',
        error_message=(
            'YAML support requires optional dependency "PyYAML".\n'
            'Install with: pip install PyYAML'
        ),
    )


def _require_yaml() -> None:
    """Ensure PyYAML is available or raise an informative error."""
    _get_yaml()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONData:
    """
    Read YAML content from ``path``.

    Validates that the YAML root is a dict or a list of dicts.

    Parameters
    ----------
    path : Path
        Path to the YAML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the YAML file.

    Raises
    ------
    TypeError
        If the YAML root is not an object or an array of objects.
    """
    _require_yaml()

    with path.open('r', encoding='utf-8') as handle:
        loaded = _get_yaml().safe_load(handle)

    if isinstance(loaded, dict):
        return cast(JSONDict, loaded)
    if isinstance(loaded, list):
        if all(isinstance(item, dict) for item in loaded):
            return cast(JSONList, loaded)
        raise TypeError(
            'YAML array must contain only objects (dicts) when loading',
        )
    raise TypeError(
        'YAML root must be an object or an array of objects when loading',
    )


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` as YAML to ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the YAML file on disk.
    data : JSONData
        Data to write as YAML.

    Returns
    -------
    int
        The number of records written.
    """
    _require_yaml()
    with path.open('w', encoding='utf-8') as handle:
        _get_yaml().safe_dump(
            data,
            handle,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
    return count_records(data)
