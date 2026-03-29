"""
:mod:`etlplus.storage._s3` module.

AWS S3 storage backend.
"""

from __future__ import annotations

from importlib import import_module
from typing import IO
from typing import Any
from typing import cast

from ._enums import StorageScheme
from ._location import StorageLocation
from ._remote import RemoteStorageBackend
from ._remote_buffer import open_remote_buffer
from ._remote_buffer import parse_remote_open_mode

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'S3StorageBackend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_boto3() -> Any:
    """
    Import and return the boto3 module.

    Returns
    -------
    Any
        Imported boto3 module.

    Raises
    ------
    ImportError
        If boto3 is not installed.
    """
    try:
        return import_module('boto3')
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'S3 storage support requires optional dependency "boto3".\n'
            'Install with: pip install boto3',
        ) from e


# SECTION: CLASSES ========================================================== #


class S3StorageBackend(RemoteStorageBackend):
    """
    Storage backend for ``s3://bucket/key`` locations.

    Runtime operations use boto3 and the default AWS credential chain.
    """

    # -- Class Attributes -- #

    authority_label = 'bucket name'
    package_name = 'boto3'
    path_label = 'object key'
    scheme = StorageScheme.S3
    service_name = 'S3'

    # -- Internal Instance Methods -- #

    def _client(self) -> Any:
        """Return one boto3 S3 client using the default credential chain."""
        boto3 = _import_boto3()
        return cast(Any, boto3.client('s3'))

    def _is_not_found_error(
        self,
        error: Exception,
    ) -> bool:
        """
        Return whether one S3 client error represents a missing object.

        Parameters
        ----------
        error : Exception
            Client error raised by boto3.

        Returns
        -------
        bool
            ``True`` when the error represents a missing object.

        """
        response = getattr(error, 'response', None)
        if not isinstance(response, dict):
            return False
        error_dict = response.get('Error', {})
        code = str(error_dict.get('Code', '')).strip()
        return code in {'404', 'NoSuchBucket', 'NoSuchKey', 'NotFound'}

    # -- Instance Methods -- #

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Delete one S3 object.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)
        self._client().delete_object(
            Bucket=location.authority,
            Key=location.path,
        )

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether one S3 object exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the S3 object exists.

        Raises
        ------
        Exception
            Propagated client errors other than not-found responses.
        """
        self._validate(location)
        client = self._client()
        try:
            client.head_object(
                Bucket=location.authority,
                Key=location.path,
            )
        except Exception as e:
            if self._is_not_found_error(e):
                return False
            raise
        return True

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open one S3 object via an in-memory file-like buffer.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Remote open mode. Supports ``r``, ``rb``, ``rt``, ``w``,
            ``wb``, and ``wt``.
        **kwargs : Any
            Text-mode options such as ``encoding``, ``errors``, and
            ``newline``. Write mode also accepts ``content_type``.

        Returns
        -------
        IO[Any]
            In-memory file-like object backed by S3 download or upload calls.

        Raises
        ------
        TypeError
            If unsupported keyword arguments are provided.
        """
        self._validate(location)
        kind, text_mode = parse_remote_open_mode(mode)
        client = self._client()
        encoding = kwargs.pop('encoding', 'utf-8')
        errors = kwargs.pop('errors', None)
        newline = kwargs.pop('newline', None)
        content_type = kwargs.pop('content_type', None)
        if kwargs:
            unexpected = ', '.join(sorted(kwargs))
            raise TypeError(
                f'Unsupported S3 open() keyword arguments: {unexpected}',
            )

        if kind == 'read':
            response = client.get_object(
                Bucket=location.authority,
                Key=location.path,
            )
            payload = response['Body'].read()
            return open_remote_buffer(
                kind='read',
                text_mode=text_mode,
                payload=payload,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )

        def _uploader(payload: bytes) -> None:
            put_kwargs = {
                'Body': payload,
                'Bucket': location.authority,
                'Key': location.path,
            }
            if content_type is not None:
                put_kwargs['ContentType'] = content_type
            client.put_object(**put_kwargs)

        return open_remote_buffer(
            kind='write',
            text_mode=text_mode,
            uploader=_uploader,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
