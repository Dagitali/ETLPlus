"""
:mod:`etlplus.file.yaml` module.

Helpers for reading/writing YAML Ain't Markup Language (YAML) files.

Notes
-----
- A YAML file is a human-readable data serialization format.
- Common cases:
    - Configuration files.
    - Data exchange between languages with different data structures.
    - Complex data storage.
- Rule of thumb:
    - If the file follows the YAML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ..types import JSONData
from ..types import StrPath
from ..utils import count_records
from ._imports import get_yaml
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read YAML content from *path*.

    Validates that the YAML root is a dict or a list of dicts.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the YAML file.
    """
    path = coerce_path(path)
    with path.open('r', encoding='utf-8') as handle:
        loaded = get_yaml().safe_load(handle)

    return coerce_record_payload(loaded, format_name='YAML')


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* as YAML to *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.
    data : JSONData
        Data to write as YAML.

    Returns
    -------
    int
        The number of records written.
    """
    path = coerce_path(path)
    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8') as handle:
        get_yaml().safe_dump(
            data,
            handle,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
    return count_records(data)
