"""
:mod:`tests.integration.test_i_examples_data_parity` module.

Sample data integration test suite. Ensures that example input data files
in different formats contain identical records.

Notes
-----
- Compares sample CSV and JSON files in the examples/data directory.
- Normalizes data types for accurate comparison.
"""

from __future__ import annotations

from operator import itemgetter
from pathlib import Path
from typing import cast

import pytest

from etlplus.file import File
from etlplus.types import JSONDict

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


def _norm_record(
    rec: JSONDict,
) -> JSONDict:
    """Normalize record fields to consistent types for comparison."""
    return {
        'name': rec['name'],
        'email': rec['email'],
        'age': int(rec['age']),
        'status': rec['status'],
    }


# SECTION: TESTS ============================================================ #


def test_examples_sample_csv_json_parity_integration():
    """Test that example CSV and JSON sample data contain identical records."""
    repo_root = Path(__file__).resolve().parents[2]
    source_dir = repo_root / 'examples' / 'data'
    csv_path = source_dir / 'sample.csv'
    json_path = source_dir / 'sample.json'

    assert csv_path.exists(), f'Missing CSV fixture: {csv_path}'
    assert json_path.exists(), f'Missing JSON fixture: {json_path}'

    csv_data = File(csv_path).read()
    json_data = File(json_path).read()

    assert isinstance(csv_data, list), 'CSV should load as a list of dicts'
    assert isinstance(json_data, list), 'JSON should load as a list of dicts'

    expected_fields = {'name', 'email', 'age', 'status'}

    csv_records = cast(list[JSONDict], csv_data)
    json_records = cast(list[JSONDict], json_data)
    csv_norm = [_norm_record(r) for r in csv_records]
    json_norm = [_norm_record(r) for r in json_records]

    # Schema checks (CSV header + JSON object keys).
    for r in csv_norm:
        assert set(r.keys()) == expected_fields
    for r in json_norm:
        assert set(r.keys()) == expected_fields

    sort_key = itemgetter('email', 'name')

    assert sorted(csv_norm, key=sort_key) == sorted(
        json_norm,
        key=sort_key,
    ), 'CSV and JSON records must be identical'
