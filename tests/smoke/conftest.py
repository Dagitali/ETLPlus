"""
:mod:`tests.smoke.conftest` module.

Shared fixtures for smoke tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import JsonFactory

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='sample_records',
    params=[
        pytest.param(
            [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
            id='two-records',
        ),
        pytest.param(
            [
                {'id': 99, 'name': 'Grace'},
            ], id='single-record',
        ),
    ],
)
def sample_records_fixture(
    request: pytest.FixtureRequest,
) -> list[dict[str, Any]]:
    """
    Return representative record payloads for smoke tests.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest fixture request carrying the parametrized payload.

    Returns
    -------
    list[dict[str, Any]]
        The sample record payload.
    """
    return list(request.param)


@pytest.fixture(name='sample_records_json')
def sample_records_json_fixture(
    sample_records: list[dict[str, Any]],
) -> str:
    """Return sample records serialized as JSON."""
    return json.dumps(sample_records)


@pytest.fixture(name='json_payload_file')
def json_payload_file_fixture(
    json_file_factory: JsonFactory,
    sample_records: list[dict[str, Any]],
) -> Path:
    """Persist ``sample_records`` as JSON and return the file path."""
    return json_file_factory(sample_records, filename='records.json')


@pytest.fixture(name='rules_json')
def rules_json_fixture() -> str:
    """Return simple validation rules as a JSON string."""
    rules = {
        'id': {'type': 'integer', 'min': 0},
        'name': {'type': 'string', 'minLength': 1},
    }
    return json.dumps(rules)


@pytest.fixture(name='operations_json')
def operations_json_fixture() -> str:
    """Return a basic transform operation payload as JSON."""
    return json.dumps({'select': ['id']})
