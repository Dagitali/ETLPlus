"""
:mod:`etlplus.file.dat` module.

Helpers for reading/writing data (DAT) files.

Notes
-----
- A “DAT-formatted” file is a generic data file that may use various
    delimiters or fixed-width formats.
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
from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ._io import write_delimited

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read DAT content from *path*.

    Parameters
    ----------
    path : Path
        Path to the DAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DAT file.
    """
    with path.open('r', encoding='utf-8', newline='') as handle:
        sample = handle.read(4096)
        handle.seek(0)
        sniffer = csv.Sniffer()
        dialect: csv.Dialect
        try:
            dialect = cast(
                csv.Dialect,
                sniffer.sniff(sample, delimiters=',\t|;'),
            )
        except csv.Error:
            dialect = cast(csv.Dialect, csv.get_dialect('excel'))
        try:
            has_header = sniffer.has_header(sample)
        except csv.Error:
            has_header = True

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
    path: Path,
    data: JSONData,
) -> int:
    """
    Write *data* to DAT file at *path* and return record count.

    Parameters
    ----------
    path : Path
        Path to the DAT file on disk.
    data : JSONData
        Data to write as DAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DAT file.
    """
    return write_delimited(path, data, delimiter=',')
