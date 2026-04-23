"""
:mod:`tests.unit.ops.test_u_ops_mappings` module.

Unit tests for :mod:`etlplus.ops._mappings`.
"""

from __future__ import annotations

import importlib

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


mappings_mod = importlib.import_module('etlplus.ops._mappings')


# SECTION: TESTS ============================================================ #


class TestMappingHelpers:
    """Unit tests for mapping/indexing helper functions."""

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
