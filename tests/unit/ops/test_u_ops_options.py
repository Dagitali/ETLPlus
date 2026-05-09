"""
:mod:`tests.unit.ops.test_u_ops_options` module.

Unit tests for :mod:`etlplus.ops._options`.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


options_mod = importlib.import_module('etlplus.ops._options')


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='option_case',
    params=[
        pytest.param(
            {
                'coerce': options_mod.coerce_read_options,
                'mapping': {
                    'encoding': 'utf-16',
                    'sheet': 2,
                    'table': 123,
                    'dataset': 456,
                    'inner_name': 789,
                    'delimiter': '|',
                },
                'option_type': ReadOptions,
                'expected_attrs': {
                    'encoding': 'utf-16',
                    'sheet': 2,
                    'table': '123',
                    'dataset': '456',
                    'inner_name': '789',
                },
                'expected_extras': {'delimiter': '|'},
                'instance': ReadOptions(
                    encoding='utf-8',
                    table='people',
                    extras={'delimiter': ','},
                ),
            },
            id='read-options',
        ),
        pytest.param(
            {
                'coerce': options_mod.coerce_write_options,
                'mapping': {
                    'encoding': 65001,
                    'root_tag': 7,
                    'sheet': 'Summary',
                    'table': 123,
                    'dataset': 456,
                    'inner_name': 789,
                    'indent': 2,
                },
                'option_type': WriteOptions,
                'expected_attrs': {
                    'encoding': '65001',
                    'root_tag': '7',
                    'sheet': 'Summary',
                    'table': '123',
                    'dataset': '456',
                    'inner_name': '789',
                },
                'expected_extras': {'indent': 2},
                'instance': WriteOptions(
                    encoding='utf-8',
                    root_tag='records',
                    extras={'indent': 4},
                ),
            },
            id='write-options',
        ),
    ],
)
def option_case_fixture(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Provide parametrized read/write option coercion cases."""
    return request.param


# SECTION: TESTS ============================================================ #


class TestFileOptionHelpers:
    """Unit tests for file-option normalization helpers."""

    def test_coerce_file_options_from_mapping(
        self,
        option_case: dict[str, Any],
    ) -> None:
        """
        Test that mapping inputs are normalized into concrete option objects.
        """
        case = option_case
        options = case['coerce'](case['mapping'])

        assert isinstance(options, case['option_type'])
        assert options is not None
        for attr_name, expected_value in case['expected_attrs'].items():
            assert getattr(options, attr_name) == expected_value
        assert options.extras == case['expected_extras']

    def test_coerce_file_options_passthrough(
        self,
        option_case: dict[str, Any],
    ) -> None:
        """Test that existing option objects are returned unchanged."""
        case = option_case
        assert case['coerce'](case['instance']) is case['instance']

    def test_internal_coerce_file_options_rejects_invalid_object(self) -> None:
        """
        Test that invalid non-mapping option objects raise :class:`TypeError`.
        """
        with pytest.raises(TypeError, match='options must be a mapping'):
            options_mod._coerce_file_options(
                object(),
                option_type=ReadOptions,
                factory=ReadOptions,
                defaults={'encoding': 'utf-8'},
            )

    @pytest.mark.parametrize(
        ('mapping', 'expected_encoding', 'expected_root_tag'),
        [
            pytest.param({}, 'utf-8', None, id='read-default-encoding'),
            pytest.param(
                {'encoding': 'utf-16'},
                'utf-16',
                None,
                id='read-string-encoding',
            ),
            pytest.param(
                {'encoding': 65001},
                '65001',
                None,
                id='read-stringify-encoding',
            ),
            pytest.param(
                {'encoding': None, 'root_tag': None},
                'utf-8',
                'root',
                id='write-defaults',
            ),
            pytest.param(
                {'encoding': 65001, 'root_tag': 7},
                '65001',
                '7',
                id='write-stringify-required-text',
            ),
        ],
    )
    def test_required_text_fields_are_normalized_through_option_coercion(
        self,
        mapping: dict[str, object],
        expected_encoding: str,
        expected_root_tag: str | None,
    ) -> None:
        """Test required text fields default or stringify through option coercion."""
        read_options = options_mod.coerce_read_options(mapping)

        assert read_options is not None
        assert read_options.encoding == expected_encoding

        write_options = options_mod.coerce_write_options(mapping)

        assert write_options is not None
        assert write_options.encoding == expected_encoding
        if expected_root_tag is not None:
            assert write_options.root_tag == expected_root_tag
