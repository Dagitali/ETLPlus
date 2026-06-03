"""
:mod:`tests.unit.storage.test_u_storage_hdfs` module.

Unit tests for :mod:`etlplus.storage._hdfs`.
"""

from __future__ import annotations

from io import BytesIO

import pytest

from etlplus.storage import HdfsStorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _hdfs as hdfs_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHdfsStorageBackend:
    """Unit tests for :class:`etlplus.storage.HdfsStorageBackend`."""

    def test_delete_uses_fsspec_filesystem(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS delete delegates to fsspec."""
        backend = HdfsStorageBackend()
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/data/table.parquet',
        )
        deletes: list[str] = []

        class FakeFilesystem:
            """fsspec filesystem delete test double."""

            def rm(self, raw: str) -> None:
                """Record the delete target."""
                deletes.append(raw)

        monkeypatch.setattr(
            backend,
            '_filesystem',
            lambda _location: FakeFilesystem(),
        )

        backend.delete(location)

        assert deletes == [
            'hdfs://namenode.example.com:8020/data/table.parquet',
        ]

    def test_ensure_parent_dir_creates_hdfs_parent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS parent preparation delegates to fsspec."""
        backend = HdfsStorageBackend()
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/data/table.parquet',
        )
        mkdirs: list[dict[str, object]] = []

        class FakeFilesystem:
            """fsspec filesystem mkdirs test double."""

            def mkdirs(self, path: str, **kwargs: object) -> None:
                """Record the mkdirs target."""
                mkdirs.append({'path': path, **kwargs})

        monkeypatch.setattr(
            backend,
            '_filesystem',
            lambda _location: FakeFilesystem(),
        )

        backend.ensure_parent_dir(location)

        assert mkdirs == [
            {
                'path': 'hdfs://namenode.example.com:8020/data',
                'exist_ok': True,
            },
        ]

    def test_ensure_parent_dir_noops_for_root_child_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS paths without a parent do not call mkdirs."""
        backend = HdfsStorageBackend()
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/table.parquet',
        )
        calls: list[StorageLocation] = []

        def _filesystem(_location: StorageLocation) -> object:
            calls.append(_location)
            return object()

        monkeypatch.setattr(backend, '_filesystem', _filesystem)

        backend.ensure_parent_dir(location)

        assert calls == []

    def test_exists_raises_import_error_without_fsspec(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS runtime needs the optional fsspec package."""
        backend = HdfsStorageBackend()
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/data/table.parquet',
        )
        monkeypatch.setattr(
            hdfs_mod,
            'import_module',
            lambda _: (_ for _ in ()).throw(ImportError),
        )

        with pytest.raises(ImportError, match='fsspec'):
            backend.exists(location)

    def test_exists_uses_fsspec_filesystem(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS exists delegates to fsspec."""
        backend = HdfsStorageBackend(token='secret')
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/data/table.parquet',
        )
        calls: list[tuple[str, dict[str, object]]] = []

        class FakeFilesystem:
            """fsspec filesystem test double."""

            def exists(self, raw: str) -> bool:
                """Record the existence check."""
                calls.append((raw, {}))
                return True

        class FakeFsspec:
            """fsspec module test double."""

            @staticmethod
            def filesystem(protocol: str, **kwargs: object) -> FakeFilesystem:
                """Return one fake filesystem."""
                calls.append((protocol, kwargs))
                return FakeFilesystem()

        monkeypatch.setattr(hdfs_mod, '_import_fsspec', lambda: FakeFsspec)

        assert backend.exists(location) is True
        assert calls == [
            ('hdfs', {'token': 'secret'}),
            ('hdfs://namenode.example.com:8020/data/table.parquet', {}),
        ]

    def test_import_fsspec_returns_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that fsspec is returned from the import helper."""
        module = object()
        monkeypatch.setattr(hdfs_mod, 'import_module', lambda _: module)

        assert hdfs_mod._import_fsspec() is module

    def test_open_uses_fsspec_open(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that HDFS open delegates to fsspec."""
        backend = HdfsStorageBackend(token='secret')
        location = StorageLocation.from_value(
            'hdfs://namenode.example.com:8020/data/table.txt',
        )
        calls: list[dict[str, object]] = []

        class FakeOpen:
            """fsspec open-file test double."""

            def open(self) -> BytesIO:
                """Return a readable payload."""
                return BytesIO(b'payload')

        class FakeFsspec:
            """fsspec module test double."""

            @staticmethod
            def open(raw: str, **kwargs: object) -> FakeOpen:
                """Record the open call."""
                calls.append({'raw': raw, **kwargs})
                return FakeOpen()

        monkeypatch.setattr(hdfs_mod, '_import_fsspec', lambda: FakeFsspec)

        with backend.open(location, 'rb', block_size=1024) as handle:
            assert handle.read() == b'payload'

        assert calls == [
            {
                'raw': 'hdfs://namenode.example.com:8020/data/table.txt',
                'mode': 'rb',
                'token': 'secret',
                'block_size': 1024,
            },
        ]
