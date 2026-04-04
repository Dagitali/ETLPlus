"""
:mod:`tests.unit.ops.test_u_ops_shared` module.

Unit tests for :mod:`etlplus.ops._shared`.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any

import pytest

from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=protected-access

# SECTION: HELPERS ========================================================== #


shared_mod = importlib.import_module('etlplus.ops._shared')


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    params=[
        pytest.param(
            {
                'coerce': shared_mod.coerce_read_options,
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
                'coerce': shared_mod.coerce_write_options,
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
def option_case(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Provide parametrized read/write option coercion cases."""
    return request.param


# SECTION: TESTS ============================================================ #


class TestSharedFileOptionHelpers:
    """Unit tests for shared file-option normalization helpers."""

    def test_coerce_file_options_from_mapping(
        self,
        option_case: dict[str, Any],
    ) -> None:
        """Mapping inputs should be normalized into concrete option objects."""
        options = option_case['coerce'](option_case['mapping'])

        assert isinstance(options, option_case['option_type'])
        assert options is not None
        for attr_name, expected_value in option_case['expected_attrs'].items():
            assert getattr(options, attr_name) == expected_value
        assert options.extras == option_case['expected_extras']

    def test_coerce_file_options_passthrough(
        self,
        option_case: dict[str, Any],
    ) -> None:
        """Existing option objects should be returned unchanged."""
        assert option_case['coerce'](option_case['instance']) is option_case['instance']


class TestSharedCollectionHelpers:
    """Unit tests for shared indexing and mapping merge helpers."""

    def test_index_named_items_rejects_duplicates(self) -> None:
        """Duplicate names should raise a descriptive error."""
        items = [
            SimpleNamespace(name='dup'),
            SimpleNamespace(name='dup'),
        ]

        with pytest.raises(ValueError, match='Duplicate source connector name'):
            shared_mod.index_named_items(
                items,
                item_label='source connector',
            )

    def test_index_named_items_skips_blank_or_missing_names(self) -> None:
        """Items without usable names should be ignored."""
        items = [
            SimpleNamespace(name='valid'),
            SimpleNamespace(name=''),
            SimpleNamespace(),
        ]

        indexed = shared_mod.index_named_items(
            items,
            item_label='job',
        )

        assert indexed == {'valid': items[0]}

    def test_merge_mapping_options_excludes_reserved_keys(self) -> None:
        """Later mappings should win and excluded keys should be removed."""
        merged = shared_mod.merge_mapping_options(
            {'encoding': 'utf-8', 'path': '/tmp/a.json'},
            {'delimiter': ';', 'path': '/tmp/b.json', 'format': 'csv'},
            excluded_keys=frozenset({'path', 'format'}),
        )

        assert merged == {
            'delimiter': ';',
            'encoding': 'utf-8',
        }
