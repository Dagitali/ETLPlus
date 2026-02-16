"""Shared roundtrip-case builders/constants for file unit tests."""

from __future__ import annotations

from etlplus.types import JSONData

from .pytest_file_contract_mixins import RoundtripSpec

# SECTION: FUNCTIONS ======================================================== #


def build_delimited_roundtrip_spec() -> RoundtripSpec:
    """
    Build a simple roundtrip spec for delimited formats.

    Returns
    -------
    RoundtripSpec
        A roundtrip specification with a simple record containing an ID and
        name.
    """
    return RoundtripSpec(
        payload=[{'id': 1, 'name': 'Ada'}],
        expected=[{'id': '1', 'name': 'Ada'}],
    )


def build_two_id_records_roundtrip_spec() -> RoundtripSpec:
    """
    Build a roundtrip spec with two ID records.

    Returns
    -------
    RoundtripSpec
        A roundtrip specification with two records, each containing an ID.
    """
    return RoundtripSpec(
        payload=[{'id': 1}, {'id': 2}],
        expected=[{'id': 1}, {'id': 2}],
    )


def build_template_roundtrip_spec(template: str) -> RoundtripSpec:
    """
    Build a roundtrip spec with a single template record.

    Parameters
    ----------
    template : str
        The template string to include in the payload and expected data.

    Returns
    -------
    RoundtripSpec
        The constructed roundtrip specification with the template.
    """
    return RoundtripSpec(
        payload={'template': template},
        expected=[{'template': template}],
    )


def build_text_alpha_beta_roundtrip_spec() -> RoundtripSpec:
    """
    Build a roundtrip spec with two text records.

    Returns
    -------
    RoundtripSpec
        A roundtrip specification with two records, each containing a text
        field with values 'alpha' and 'beta'.
    """
    return RoundtripSpec(
        payload=[{'text': 'alpha'}, {'text': 'beta'}],
        expected=[{'text': 'alpha'}, {'text': 'beta'}],
    )


def build_roundtrip_spec(
    payload: JSONData,
    expected: JSONData,
) -> RoundtripSpec:
    """
    Build a roundtrip spec with arbitrary payload and expected data.

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
    return RoundtripSpec(payload=payload, expected=expected)
