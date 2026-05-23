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

from etlplus.file import File
from etlplus.utils._types import JSONDict

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


EXAMPLES_DATA_DIR = Path(__file__).resolve().parents[2] / 'examples' / 'data'
EXPECTED_SAMPLE_FIELDS = {'name', 'email', 'age', 'status'}


# SECTION: TESTS ============================================================ #


def test_examples_sample_csv_json_parity_integration() -> None:
    """Test that example CSV and JSON sample data contain identical records."""
    csv_path = EXAMPLES_DATA_DIR / 'sample.csv'
    json_path = EXAMPLES_DATA_DIR / 'sample.json'

    assert csv_path.exists(), f'Missing CSV fixture: {csv_path}'
    assert json_path.exists(), f'Missing JSON fixture: {json_path}'

    csv_data = File(csv_path).read()
    json_data = File(json_path).read()

    assert isinstance(csv_data, list), 'CSV should load as a list of dicts'
    assert isinstance(json_data, list), 'JSON should load as a list of dicts'

    csv_records = cast(list[JSONDict], csv_data)
    json_records = cast(list[JSONDict], json_data)
    normalized_by_source = [
        [
            {
                'name': record['name'],
                'email': record['email'],
                'age': int(record['age']),
                'status': record['status'],
            }
            for record in records
        ]
        for records in (csv_records, json_records)
    ]

    # Schema checks (CSV header + JSON object keys).
    assert all(
        set(record) == EXPECTED_SAMPLE_FIELDS
        for records in normalized_by_source
        for record in records
    )

    sort_key = itemgetter('email', 'name')
    csv_norm, json_norm = normalized_by_source

    assert sorted(csv_norm, key=sort_key) == sorted(
        json_norm,
        key=sort_key,
    ), 'CSV and JSON records must be identical'
