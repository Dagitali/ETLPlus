"""
:mod:`tests.unit.ops.test_u_ops_mappings` module.

Unit tests for :mod:`etlplus.ops._mappings`.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


mappings_mod = importlib.import_module('etlplus.ops._mappings')


# SECTION: TESTS ============================================================ #


class TestMappingHelpers:
    """Unit tests for mapping/indexing helper functions."""

    def test_index_named_items_rejects_duplicates(self) -> None:
        """Duplicate names should raise a descriptive error."""
        items = [
            SimpleNamespace(name='dup'),
            SimpleNamespace(name='dup'),
        ]

        with pytest.raises(ValueError, match='Duplicate source connector name'):
            mappings_mod.index_named_items(
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

        indexed = mappings_mod.index_named_items(
            items,
            item_label='job',
        )

        assert indexed == {'valid': items[0]}

    def test_merge_mapping_options_excludes_reserved_keys(self) -> None:
        """Later mappings should win and excluded keys should be removed."""
        merged = mappings_mod.merge_mapping_options(
            {'encoding': 'utf-8', 'path': '/tmp/a.json'},
            {'delimiter': ';', 'path': '/tmp/b.json', 'format': 'csv'},
            excluded_keys=frozenset({'path', 'format'}),
        )

        assert merged == {
            'delimiter': ';',
            'encoding': 'utf-8',
        }
