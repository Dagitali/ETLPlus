"""
:mod:`etlplus.file.tsv` module.

Helpers for reading/writing TSV files.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'read',
    'write',
]


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: Path,
) -> JSONList:
    """
    Read TSV content from ``path``.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the TSV file.
    """
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(handle, delimiter='\t')
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def write(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write ``data`` to TSV at ``path`` and return record count.

    Parameters
    ----------
    path : Path
        Path to the TSV file on disk.
    data : JSONData
        Data to write as TSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the TSV file.
    """
    rows: list[JSONDict]
    if isinstance(data, list):
        rows = [row for row in data if isinstance(row, dict)]
    else:
        rows = [data]

    if not rows:
        return 0

    fieldnames = sorted({key for row in rows for key in row})
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    return len(rows)
