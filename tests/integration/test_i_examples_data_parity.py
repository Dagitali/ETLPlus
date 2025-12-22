"""
:mod:`tests.integration.test_i_examples_data_parity` module.

Sample data integration test suite. Ensures that example input data files
in different formats contain identical records.

Notes
-----
- Compares sample CSV and JSON files in the examples/data directory.
- Normalizes data types for accurate comparison.
"""

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import File

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


def _norm_record(
    rec: dict[str, Any],
) -> dict[str, Any]:
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

    csv_data = File.read_file(csv_path)
    json_data = File.read_file(json_path)

    assert isinstance(csv_data, list), 'CSV should load as a list of dicts'
    assert isinstance(json_data, list), 'JSON should load as a list of dicts'

    expected_fields = {'name', 'email', 'age', 'status'}

    csv_norm = [_norm_record(r) for r in csv_data]  # type: ignore[arg-type]
    json_norm = [_norm_record(r) for r in json_data]  # type: ignore[arg-type]

    # Schema checks (CSV header + JSON object keys).
    for r in csv_norm:
        assert set(r.keys()) == expected_fields
    for r in json_norm:
        assert set(r.keys()) == expected_fields

    def sort_key(r: dict[str, Any]):
        # Email unique in fixtures.
        return (
            r['email'],
            r['name'],
        )

    assert sorted(csv_norm, key=sort_key) == sorted(
        json_norm,
        key=sort_key,
    ), 'CSV and JSON records must be identical'
