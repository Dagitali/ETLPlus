"""
:mod:`tests.unit.file.pytest_file_roundtrip_spec` module.

Roundtrip specification data model and generation helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions
from etlplus.types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'RoundtripShape',
    'RoundtripSpec',
    'RoundtripValueKind',
]


# SECTION: TYPE ALIASES ===================================================== #


type RoundtripShape = Literal[
    'records',
    'delimited',
    'template',
    'text',
    'mapping',
]
type RoundtripValueKind = Literal['numeric', 'string', 'mixed']


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class RoundtripSpec:
    """
    Declarative roundtrip case for one format-aligned unit contract.
    """

    payload: JSONData
    expected: JSONData
    stem: str = 'roundtrip'
    read_options: ReadOptions | None = None
    write_options: WriteOptions | None = None
    expected_written_count: int | None = None

    @classmethod
    def build(
        cls,
        *,
        shape: RoundtripShape = 'records',
        field_count: int = 1,
        record_count: int = 1,
        value_kind: RoundtripValueKind = 'numeric',
    ) -> RoundtripSpec:
        """
        Build a generated roundtrip spec.

        Generated record field names are standardized to ``id``, ``name``,
        and ``age`` before falling back to ``field_N``.

        Parameters
        ----------
        shape : RoundtripShape, optional
            Shape of the payload/expected data, by default 'records'.
        field_count : int, optional
            Number of fields per record, by default 1.
        record_count : int, optional
            Number of records, by default 1.
        value_kind : RoundtripValueKind, optional
            Kind of values to generate, by default 'numeric'.

        Returns
        -------
        RoundtripSpec
            The generated roundtrip specification.

        Raises
        ------
        ValueError
            If *field_count* or *record_count* is less than 1.
        """
        if field_count < 1 or record_count < 1:
            raise ValueError('field_count and record_count must be >= 1')

        names = ('Alice', 'Bob', 'Cleo', 'Dora', 'Evan', 'Faye')
        words = ('alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta')

        def _field_name(field_index: int) -> str:
            if field_index == 0:
                return 'id'
            if field_index == 1:
                return 'name'
            if field_index == 2:
                return 'age'
            return f'field_{field_index + 1}'

        def _base_value(row_index: int, field_index: int) -> int | str:
            field_name = _field_name(field_index)
            if field_name == 'id':
                return row_index + 1
            if field_name == 'name':
                return names[row_index % len(names)]
            if field_name == 'age':
                return 20 + row_index
            return row_index + field_index + 1

        def _coerce_value(row_index: int, field_index: int) -> int | str:
            base_value = _base_value(row_index, field_index)
            if value_kind == 'numeric':
                return (
                    base_value
                    if isinstance(base_value, int)
                    else row_index + field_index + 1
                )
            if value_kind == 'string':
                return str(base_value)
            return base_value

        if shape == 'template':
            template_row = {'template': 'template_1'}
            return cls(payload=template_row, expected=[template_row])

        if shape == 'text':
            text_rows = [
                {
                    'text': (
                        str(row_index + 1)
                        if value_kind == 'numeric'
                        else (
                            words[row_index % len(words)]
                            if value_kind == 'mixed'
                            else f'text_{row_index + 1}'
                        )
                    ),
                }
                for row_index in range(record_count)
            ]
            return cls(payload=text_rows, expected=text_rows)

        if shape == 'mapping':
            payload_map: dict[str, int | str] = {
                _field_name(field_index): _coerce_value(0, field_index)
                for field_index in range(field_count)
            }
            return cls(payload=payload_map, expected=payload_map)

        generated_rows: list[dict[str, int | str]] = []
        for row_index in range(record_count):
            row_payload: dict[str, int | str] = {}
            for field_index in range(field_count):
                row_payload[_field_name(field_index)] = _coerce_value(
                    row_index,
                    field_index,
                )
            generated_rows.append(row_payload)

        expected_rows: JSONData
        if shape == 'delimited':
            expected_rows = [
                {key: str(value) for key, value in row_data.items()}
                for row_data in generated_rows
            ]
        else:
            expected_rows = generated_rows
        return cls(payload=generated_rows, expected=expected_rows)
