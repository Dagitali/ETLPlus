"""
:mod:`etlplus.file.orc` module.

Helpers for reading/writing ORC files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_PANDAS_CACHE: dict[str, Any] = {}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _get_pandas() -> Any:
    """
    Return the pandas module, importing it on first use.

    Raises an informative ImportError if the optional dependency is missing.
    """
    mod = _PANDAS_CACHE.get('mod')
    if mod is not None:  # pragma: no cover - tiny branch
        return mod
    try:
        _pd = __import__('pandas')  # type: ignore[assignment]
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ORC support requires optional dependency "pandas".\n'
            'Install with: pip install pandas',
        ) from e
    _PANDAS_CACHE['mod'] = _pd

    return _pd


def _normalize_records(data: JSONData) -> JSONList:
    """
    Normalize JSON payloads into a list of dictionaries.

    Raises TypeError when payloads contain non-dict items.
    """
    if isinstance(data, list):
        if not all(isinstance(item, dict) for item in data):
            raise TypeError('ORC payloads must contain only objects (dicts)')
        return cast(JSONList, data)
    return [cast(JSONDict, data)]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read ORC content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the ORC file.

    Raises
    ------
    ImportError
        When optional dependency "pyarrow" is missing.
    """
    pandas = _get_pandas()
    try:
        frame = pandas.read_orc(path)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ORC support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return cast(JSONList, frame.to_dict(orient='records'))


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to ORC at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the ORC file on disk.
    data : JSONData
        Data to write.

    Returns
    -------
    int
        Number of records written.

    Raises
    ------
    ImportError
        When optional dependency "pyarrow" is missing.
    """
    records = _normalize_records(data)
    if not records:
        return 0

    pandas = _get_pandas()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pandas.DataFrame.from_records(records)
    try:
        frame.to_orc(path, index=False)
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ORC support requires optional dependency "pyarrow".\n'
            'Install with: pip install pyarrow',
        ) from e
    return len(records)
