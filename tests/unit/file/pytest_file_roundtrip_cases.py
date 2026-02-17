"""
:mod:`tests.unit.file.pytest_file_roundtrip_cases` module.

Roundtrip spec builders for file unit tests.
"""

from __future__ import annotations

from etlplus.types import JSONData

from .pytest_file_contract_mixins import RoundtripShape
from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripValueKind

# SECTION: CONSTANTS ===================================================== #


ROUNDTRIP_CASES: dict[str, tuple[JSONData, JSONData]] = {
    'dat_records': (
        [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}],
        [{'id': '1', 'name': 'Alice'}, {'id': '2', 'name': 'Bob'}],
    ),
    'ini_default_alpha': (
        {'DEFAULT': {'shared': 'base', 'timeout': 5}, 'alpha': {'value': 1}},
        {
            'DEFAULT': {'shared': 'base', 'timeout': '5'},
            'alpha': {'value': '1'},
        },
    ),
    'xml_nested_attributes': (
        {
            'root': {
                '@id': '42',
                'item': {'@lang': 'en', 'text': 'Hello'},
            },
        },
        {
            'root': {
                '@id': '42',
                'item': {'@lang': 'en', 'text': 'Hello'},
            },
        },
    ),
}


# SECTION: FUNCTIONS ======================================================== #


def build_roundtrip_spec(
    payload: JSONData | None = None,
    expected: JSONData | None = None,
    *,
    case: str | None = None,
    shape: RoundtripShape = 'records',
    field_count: int = 1,
    record_count: int = 1,
    value_kind: RoundtripValueKind = 'numeric',
) -> RoundtripSpec:
    """
    Build one :class:`RoundtripSpec`.

    Supports explicit ``payload``/``expected``, named ``case`` lookup, or
    generated payloads via :meth:`RoundtripSpec.build`.

    Parameters
    ----------
    payload : JSONData
        The payload to write in the roundtrip test.
    expected : JSONData
        The expected data to read in the roundtrip test.

    Returns
    -------
    RoundtripSpec
        The constructed roundtrip specification.
    """
    if (payload is None) != (expected is None):
        raise ValueError('payload and expected must be provided together')
    if payload is not None and expected is not None:
        return RoundtripSpec(payload=payload, expected=expected)
    if case is not None:
        selected_payload, selected_expected = ROUNDTRIP_CASES[case]
        return RoundtripSpec(
            payload=selected_payload,
            expected=selected_expected,
        )
    return RoundtripSpec.build(
        shape=shape,
        field_count=field_count,
        record_count=record_count,
        value_kind=value_kind,
    )
