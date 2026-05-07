"""
:mod:`etlplus.storage._azure_blob` module.

Azure Blob Storage backend.
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import IO
from typing import Any
from typing import cast

from ..utils._imports import build_dependency_error_message
from ..utils._imports import import_package
from ._enums import StorageScheme
from ._location import StorageLocation
from ._remote import RemoteStorageBackend
from ._remote_buffer import open_remote_buffer
from ._remote_buffer import parse_remote_open_mode

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AzureBlobStorageBackend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_blob_types() -> tuple[Any, Any | None]:
    """
    Import Azure Blob SDK types.

    Returns
    -------
    tuple[Any, Any | None]
        ``(BlobServiceClient, ContentSettings or None)``.
    """
    module = import_package(
        'azure.storage.blob',
        error_message=build_dependency_error_message(
            'azure.storage.blob',
            format_name='Azure Blob storage',
            pip_name='azure-storage-blob',
        ),
        importer=import_module,
    )
    return module.BlobServiceClient, getattr(module, 'ContentSettings', None)


# SECTION: CLASSES ========================================================== #


class AzureBlobStorageBackend(RemoteStorageBackend):
    """
    Storage backend for Azure Blob object locations.

    The canonical URI form for this surface is
    ``azure-blob://container/blob/path``. HTTPS object URLs in the form
    ``https://account.blob.core.windows.net/container/blob/path`` are also
    accepted. Runtime operations use :mod:`azure-storage-blob` and can be
    configured with an explicit connection string, an account URL, or the
    corresponding Azure environment variables.
    """

    # -- Class Attributes -- #

    authority_label = 'container name'
    package_name = 'azure-storage-blob'
    path_label = 'blob path'
    scheme = StorageScheme.AZURE_BLOB
    service_name = 'Azure Blob'

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        account_url: str | None = None,
        credential: object | None = None,
    ) -> None:
        self.connection_string = connection_string
        self.account_url = account_url
        self.credential = credential

    # -- Internal Instance Methods -- #

    def _blob_client(
        self,
        location: StorageLocation,
    ) -> Any:
        """
        Return one blob client for *location*.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        Any
            Azure Blob client for the specified location.
        """
        container, _ = self._split_authority(location.authority)
        return self._service_client(location).get_blob_client(
            blob=location.path,
            container=container,
        )

    def _account_url_from_authority(
        self,
        authority: str,
    ) -> str | None:
        """Derive one HTTPS account URL from an extended authority string."""
        _, account_host = self._split_authority(authority)
        if account_host is None:
            return None
        return f'https://{account_host}'

    def _split_authority(
        self,
        authority: str,
    ) -> tuple[str, str | None]:
        """Split authority into container and optional account host."""
        container, separator, account_host = authority.partition('@')
        if not container:
            raise ValueError(
                'Azure Blob locations require authority in the form '
                '"container" or "container@account.blob.core.windows.net"',
            )
        if separator and not account_host:
            raise ValueError(
                'Azure Blob locations require authority in the form '
                '"container" or "container@account.blob.core.windows.net"',
            )
        return container, account_host or None

    def _service_client(
        self,
        location: StorageLocation | None = None,
    ) -> Any:
        """Return one Azure Blob service client."""
        blob_service_client_type, _ = _import_blob_types()
        connection_string = self.connection_string or os.getenv(
            'AZURE_STORAGE_CONNECTION_STRING',
        )
        if connection_string:
            return cast(Any, blob_service_client_type).from_connection_string(
                connection_string,
            )

        account_url = self.account_url or os.getenv('AZURE_STORAGE_ACCOUNT_URL')
        if not account_url and location is not None:
            account_url = self._account_url_from_authority(location.authority)
        if not account_url:
            raise ValueError(
                'Azure Blob backend requires AZURE_STORAGE_CONNECTION_STRING '
                'or AZURE_STORAGE_ACCOUNT_URL, or an authority containing '
                'the account host',
            )

        credential = self.credential
        if credential is None:
            credential = os.getenv('AZURE_STORAGE_CREDENTIAL')
        if credential is None:
            return cast(Any, blob_service_client_type)(account_url=account_url)
        return cast(Any, blob_service_client_type)(
            account_url=account_url,
            credential=credential,
        )

    # -- Instance Methods -- #

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Delete one Azure Blob object if it exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)
        self._blob_client(location).delete_blob(delete_snapshots='include')

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether one Azure Blob exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the blob exists.
        """
        self._validate(location)
        return bool(self._blob_client(location).exists())

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open one Azure blob via an in-memory file-like buffer.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        mode : str, optional
            Remote open mode. Supports ``r``, ``rb``, ``rt``, ``w``,
            ``wb``, and ``wt``.
        **kwargs : Any
            Text-mode options such as ``encoding``, ``errors``, and
            ``newline``. Write mode accepts ``overwrite`` and ``content_type``.

        Returns
        -------
        IO[Any]
            In-memory file-like object backed by Azure Blob download or upload
            calls.

        Raises
        ------
        TypeError
            If unsupported keyword arguments are provided.
        """
        self._validate(location)
        kind, text_mode = parse_remote_open_mode(mode)
        blob_client = self._blob_client(location)
        encoding = kwargs.pop('encoding', 'utf-8')
        errors = kwargs.pop('errors', None)
        newline = kwargs.pop('newline', None)
        overwrite = kwargs.pop('overwrite', True)
        content_type = kwargs.pop('content_type', None)
        if kwargs:
            unexpected = ', '.join(sorted(kwargs))
            raise TypeError(
                f'Unsupported Azure Blob open() keyword arguments: {unexpected}',
            )

        if kind == 'read':
            payload = blob_client.download_blob().readall()
            return open_remote_buffer(
                kind='read',
                text_mode=text_mode,
                payload=payload,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )

        _, content_settings_type = _import_blob_types()

        def _uploader(payload: bytes) -> None:
            upload_kwargs: dict[str, Any] = {
                'data': payload,
                'overwrite': overwrite,
            }
            if content_type is not None and content_settings_type is not None:
                upload_kwargs['content_settings'] = content_settings_type(
                    content_type=content_type,
                )
            blob_client.upload_blob(**upload_kwargs)

        return open_remote_buffer(
            kind='write',
            text_mode=text_mode,
            uploader=_uploader,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
