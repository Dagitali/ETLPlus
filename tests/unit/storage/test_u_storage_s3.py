"""
:mod:`tests.unit.storage.test_u_storage_s3` module.

Unit tests for :mod:`etlplus.storage._s3`.
"""

from __future__ import annotations

from io import BytesIO

import pytest

from etlplus.storage import S3StorageBackend
from etlplus.storage import StorageLocation
from etlplus.storage import _s3 as s3_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestS3StorageBackend:
    """Unit tests for :class:`etlplus.storage.S3StorageBackend`."""

    def test_client_uses_imported_boto3_factory(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the S3 backend resolves the boto3 client factory."""
        marker = object()

        class FakeBoto3:
            def client(self, service: str) -> object:
                assert service == 's3'
                return marker

        backend = S3StorageBackend()
        monkeypatch.setattr(s3_mod, '_import_boto3', lambda: FakeBoto3())

        assert backend._client() is marker

    def test_delete_uses_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 delete delegates to the client."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')
        deletes: list[dict[str, object]] = []

        class FakeS3Client:
            """S3 client delete test double."""

            def delete_object(self, **kwargs: object) -> None:
                """Record object deletion arguments."""
                deletes.append(kwargs)

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())
        backend.delete(location)
        assert deletes == [{'Bucket': 'bucket', 'Key': 'data.json'}]

    def test_ensure_parent_dir_validates_bucket_and_key(self) -> None:
        """Test that invalid S3 locations fail validation early."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket')
        with pytest.raises(ValueError, match='object key'):
            backend.ensure_parent_dir(location)

    def test_exists_raises_import_error_without_sdk(self) -> None:
        """Test that S3 runtime needs the optional SDK package."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')
        with pytest.raises(ImportError, match='boto3'):
            backend.exists(location)

    def test_exists_reraises_non_not_found_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks propagate non-not-found errors."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Error(Exception):
            def __init__(self) -> None:
                self.response = {'Error': {'Code': 'AccessDenied'}}

        class FakeS3Client:
            def head_object(self, **kwargs: object) -> None:
                raise FakeS3Error()

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())

        with pytest.raises(FakeS3Error):
            backend.exists(location)

    def test_exists_returns_false_for_missing_object(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks treat not-found errors as false."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/missing.json')

        class FakeS3Error(Exception):
            """S3 not-found error test double."""

            def __init__(self) -> None:
                self.response = {'Error': {'Code': 'NoSuchKey'}}

        class FakeS3Client:
            """S3 client missing-object test double."""

            def head_object(self, **kwargs: object) -> None:
                """Raise a not-found error for the requested object."""
                raise FakeS3Error()

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())
        assert backend.exists(location) is False

    def test_exists_returns_true_when_object_is_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 existence checks return true for present objects."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Client:
            """S3 client existence test double."""

            def head_object(self, **kwargs: object) -> None:
                """Assert the requested object identity."""
                assert kwargs == {'Bucket': 'bucket', 'Key': 'data.json'}

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())
        assert backend.exists(location) is True

    def test_import_boto3_returns_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that the SDK module is returned from the S3 import helper."""
        module = object()
        monkeypatch.setattr(s3_mod, 'import_module', lambda _: module)

        assert s3_mod._import_boto3() is module

    def test_is_not_found_error_returns_false_without_mapping_response(self) -> None:
        """Test that malformed boto-style errors are not treated as missing."""
        backend = S3StorageBackend()

        class FakeS3Error(Exception):
            response = 'not-a-dict'

        assert backend._is_not_found_error(FakeS3Error()) is False

    def test_open_reads_text_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 reads return text buffers when requested."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.json')

        class FakeS3Client:
            """S3 client read test double."""

            def get_object(self, **kwargs: object) -> dict[str, object]:
                """Return the requested object payload."""
                assert kwargs == {'Bucket': 'bucket', 'Key': 'data.json'}
                return {'Body': BytesIO(b'{"ok": true}')}

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())
        with backend.open(location, encoding='utf-8') as handle:
            assert handle.read() == '{"ok": true}'

    def test_open_rejects_unexpected_kwargs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that S3 open rejects unsupported keyword arguments."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.bin')
        monkeypatch.setattr(backend, '_client', lambda: object())

        with pytest.raises(TypeError, match='Unsupported S3 open'):
            backend.open(location, 'rb', unsupported=True)

    @pytest.mark.parametrize(
        ('content_type', 'expected_extra'),
        [
            (None, {}),
            (
                'application/octet-stream',
                {'ContentType': 'application/octet-stream'},
            ),
        ],
    )
    def test_open_writes_binary_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        content_type: str | None,
        expected_extra: dict[str, str],
    ) -> None:
        """Test that S3 writes upload buffered payloads on close."""
        backend = S3StorageBackend()
        location = StorageLocation.from_value('s3://bucket/data.bin')
        uploads: list[dict[str, object]] = []

        class FakeS3Client:
            """S3 client write test double."""

            def put_object(self, **kwargs: object) -> None:
                """Record upload arguments."""
                uploads.append(kwargs)

        monkeypatch.setattr(backend, '_client', lambda: FakeS3Client())
        kwargs = {'content_type': content_type} if content_type else {}
        with backend.open(location, 'wb', **kwargs) as handle:
            handle.write(b'payload')

        assert uploads == [
            {
                'Body': b'payload',
                'Bucket': 'bucket',
                'Key': 'data.bin',
                **expected_extra,
            },
        ]
