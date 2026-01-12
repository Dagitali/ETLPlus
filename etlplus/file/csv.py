"""
:mod:`etlplus.file.csv` module.

CSV read/write helpers.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList

# SECTION: FUNCTIONS ======================================================== #


def read_csv(
    path: Path,
) -> JSONList:
    """
    Load CSV content as a list of dictionaries.

    Parameters
    ----------
    path : Path
        Path to the CSV file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the CSV file.
    """
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader: csv.DictReader[str] = csv.DictReader(handle)
        rows: JSONList = []
        for row in reader:
            if not any(row.values()):
                continue
            rows.append(cast(JSONDict, dict(row)))
    return rows


def write_csv(
    path: Path,
    data: JSONData,
) -> int:
    """
    Write CSV rows to ``path`` and return the number of rows.

    Parameters
    ----------
    path : Path
        Path to the CSV file on disk.
    data : JSONData
        Data to write as CSV. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the CSV file.
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
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    return len(rows)
