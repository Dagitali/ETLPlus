"""
:mod:`tests.unit.storage.test_u_storage_enums` module.

Unit tests for :mod:`etlplus.storage._enums`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import StorageScheme

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestStorageScheme:
    """Unit tests for :class:`etlplus.storage.StorageScheme`."""

    @pytest.mark.parametrize(
        ('alias', 'scheme'),
        [
            ('abfss', StorageScheme.ABFS),
            ('adls', StorageScheme.ABFS),
            ('azblob', StorageScheme.AZURE_BLOB),
            ('filesystem', StorageScheme.FILE),
            ('https', StorageScheme.HTTP),
            ('s3a', StorageScheme.S3),
            ('webhdfs', StorageScheme.HDFS),
        ],
    )
    def test_aliases_coerce_to_expected_scheme(
        self,
        alias: str,
        scheme: StorageScheme,
    ) -> None:
        """Test that documented scheme aliases resolve to canonical schemes."""
        assert StorageScheme.coerce(alias) is scheme

    def test_alias_mapping_contains_expected_entries(self) -> None:
        """Test that storage scheme aliases retain their public spellings."""
        assert StorageScheme.aliases() == {
            'abfss': 'abfs',
            'adls': 'abfs',
            'adls2': 'abfs',
            'azblob': 'azure-blob',
            'azureblob': 'azure-blob',
            'blob': 'azure-blob',
            'filesystem': 'file',
            'local': 'file',
            'fs': 'file',
            'https': 'http',
            's3a': 's3',
            's3n': 's3',
            'wasb': 'azure-blob',
            'wasbs': 'azure-blob',
            'webhdfs': 'hdfs',
        }
