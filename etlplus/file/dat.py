"""
:mod:`etlplus.file.dat` module.

Helpers for reading/writing data (DAT) files.

Notes
-----
- A DAT file is a generic data file that may use various delimiters or fixed-
    width formats.
- Common cases:
    - Delimited text files (e.g., CSV, TSV).
    - Fixed-width formatted files.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file does not follow a specific standard format, use this module
        for reading and writing.
"""

from __future__ import annotations

import csv
from typing import Protocol
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._io import coerce_path
from ._io import write_delimited

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_DELIMITERS = ',\t|;'


# SECTION: INTERNAL PROTOCOLS =============================================== #


class _CsvSniffer(Protocol):
    """Protocol for objects providing ``csv.Sniffer``-compatible methods."""

    def has_header(
        self,
        sample: str,
    ) -> bool: ...

    def sniff(
        self,
        sample: str,
        delimiters: str | None = None,
    ) -> csv.Dialect: ...


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _sniff(
    sample: str,
    *,
    sniffer: _CsvSniffer | None = None,
    delimiters: str = _DEFAULT_DELIMITERS,
) -> tuple[csv.Dialect, bool]:
    """
    Infer CSV dialect and header presence from an input sample.

    Parameters
    ----------
    sample : str
        Initial bytes decoded as text from the file (e.g., first 4 KiB).
    sniffer : _CsvSniffer | None, optional
        Sniffer instance used to infer the dialect/header. When omitted, a
        default :class:`csv.Sniffer` instance is created.
    delimiters : str, optional
        Candidate delimiter characters passed to :meth:`csv.Sniffer.sniff`.

    Returns
    -------
    tuple[csv.Dialect, bool]
        ``(dialect, has_header)`` where *dialect* is the inferred CSV dialect
        and *has_header* indicates whether the first non-empty row is likely a
        header row.

    Notes
    -----
    - If dialect inference fails, the Excel dialect is used as a fallback.
    - If header detection fails, the function defaults to ``True`` so the
        first row is treated as a header.
    """
    sniffer_instance = sniffer or csv.Sniffer()
    try:
        dialect = cast(
            csv.Dialect,
            sniffer_instance.sniff(sample, delimiters=delimiters),
        )
    except csv.Error:
        dialect = cast(csv.Dialect, csv.get_dialect('excel'))
    try:
        has_header = sniffer_instance.has_header(sample)
    except csv.Error:
        has_header = True
    return dialect, has_header


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Read DAT content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the DAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DAT file.
    """
    path = coerce_path(path)
    with path.open('r', encoding='utf-8', newline='') as handle:
        sample = handle.read(4096)
        handle.seek(0)
        dialect, has_header = _sniff(sample)

        reader = csv.reader(handle, dialect)
        rows = [row for row in reader if any(field.strip() for field in row)]
        if not rows:
            return []

        if has_header:
            header = rows[0]
            data_rows = rows[1:]
        else:
            header = [f'col_{i + 1}' for i in range(len(rows[0]))]
            data_rows = rows

    records: JSONList = []
    for row in data_rows:
        record: JSONDict = {}
        for index, name in enumerate(header):
            record[name] = row[index] if index < len(row) else None
        records.append(record)
    return records


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to DAT file at *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the DAT file on disk.
    data : JSONData
        Data to write as DAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DAT file.
    """
    path = coerce_path(path)
    return write_delimited(path, data, delimiter=',', format_name='DAT')
