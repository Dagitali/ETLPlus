"""
:mod:`tests.integration.file.test_i_file_csv_perf` module.

Performance-smoke coverage for a larger CSV roundtrip workflow.
"""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

import pytest

from etlplus.file.csv import CsvFile

pytestmark = [pytest.mark.integration, pytest.mark.perf]


def _large_csv_payload(
    rows: int = 50_000,
) -> list[dict[str, object]]:
    """Build a moderately large tabular payload for CSV perf smoke tests."""
    return [
        {
            'id': row,
            'name': f'user-{row:05d}',
            'active': row % 2 == 0,
            'score': row / 10,
        }
        for row in range(rows)
    ]


def test_large_csv_roundtrip_perf_smoke(
    tmp_path: Path,
) -> None:
    """Exercise a larger CSV roundtrip with a generous elapsed-time bound."""
    path = tmp_path / 'large.csv'
    payload = _large_csv_payload()
    handler = CsvFile()

    start = perf_counter()
    written = handler.write(path, payload)
    write_elapsed = perf_counter() - start

    start = perf_counter()
    loaded = handler.read(path)
    read_elapsed = perf_counter() - start

    assert written > 0
    assert isinstance(loaded, list)
    assert len(loaded) == len(payload)
    assert write_elapsed < 10
    assert read_elapsed < 10
