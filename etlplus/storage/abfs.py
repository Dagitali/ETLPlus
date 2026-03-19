"""
:mod:`etlplus.storage.abfs` module.

Azure Data Lake Storage Gen2 backend.
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import IO
from typing import Any
from typing import cast

from ._remote import RemoteStorageBackend
from ._remote_buffer import open_remote_buffer
from ._remote_buffer import parse_remote_open_mode
from .enums import StorageScheme
from .location import StorageLocation

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AbfsStorageBackend',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_datalake_types() -> tuple[Any, Any | None]:
    """
    Import Azure Data Lake SDK types.

    Returns
    -------
    tuple[Any, Any | None]
        ``(DataLakeServiceClient, ContentSettings or None)``.

    Raises
    ------
    ImportError
        If ``azure-storage-file-datalake`` is not installed.
    """
    try:
        module = import_module('azure.storage.filedatalake')
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            'ABFS storage support requires optional dependency '
            '"azure-storage-file-datalake".\n'
            'Install with: pip install azure-storage-file-datalake',
        ) from e
    return module.DataLakeServiceClient, getattr(module, 'ContentSettings', None)


# SECTION: CLASSES ========================================================== #


class AbfsStorageBackend(RemoteStorageBackend):
    """
    Storage backend for ``abfs://filesystem@account/path`` locations.

    Runtime operations use ``azure-storage-file-datalake``. The canonical
    authority shape is ``filesystem@account.dfs.core.windows.net``.
    """

    # -- Class Attributes -- #

    authority_label = 'filesystem/account authority'
    package_name = 'azure-storage-file-datalake'
    path_label = 'filesystem path'
    scheme = StorageScheme.ABFS
    service_name = 'Azure Data Lake Storage Gen2'

    # -- Magic Methods (Object Lifecycle) -- #

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

    def _account_url_from_authority(
        self,
        authority: str,
    ) -> str:
        """
        Derive one HTTPS account URL from an ABFS authority string.

        Parameters
        ----------
        authority : str
            ABFS authority string in the form ``filesystem@account-host``.

        Returns
        -------
        str
            HTTPS account URL for the specified authority.
        """
        _, account_host = self._split_authority(authority)
        return f'https://{account_host}'

    def _file_client(
        self,
        location: StorageLocation,
    ) -> Any:
        """
        Return one file client for *location*.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        Any
            Data Lake file client for the specified location.
        """
        file_system, _ = self._split_authority(location.authority)
        return self._service_client(location).get_file_client(
            file_system=file_system,
            file_path=location.path,
        )

    def _service_client(
        self,
        location: StorageLocation | None = None,
    ) -> Any:
        """
        Return one Azure Data Lake service client.

        Parameters
        ----------
        location : StorageLocation | None, optional
            Parsed storage location. Used to derive account URL if not
            explicitly provided.

        Returns
        -------
        Any
            Azure Data Lake service client instance.

        Raises
        ------
        ValueError
            If neither a connection string nor an account URL can be
            resolved from explicit configuration, environment variables, or
            the provided location authority.
        """
        service_client_type, _ = _import_datalake_types()
        connection_string = self.connection_string or os.getenv(
            'AZURE_STORAGE_CONNECTION_STRING',
        )
        if connection_string:
            return cast(Any, service_client_type).from_connection_string(
                connection_string,
            )

        account_url = self.account_url or os.getenv('AZURE_STORAGE_ACCOUNT_URL')
        if not account_url and location is not None:
            account_url = self._account_url_from_authority(location.authority)
        if not account_url:
            raise ValueError(
                'ABFS backend requires AZURE_STORAGE_CONNECTION_STRING, '
                'AZURE_STORAGE_ACCOUNT_URL, or an authority containing the '
                'account host',
            )

        credential = self.credential
        if credential is None:
            credential = os.getenv('AZURE_STORAGE_CREDENTIAL')
        if credential is None:
            return cast(Any, service_client_type)(account_url=account_url)
        return cast(Any, service_client_type)(
            account_url=account_url,
            credential=credential,
        )

    def _split_authority(
        self,
        authority: str,
    ) -> tuple[str, str]:
        """
        Split ``filesystem@account-host`` into filesystem and host.

        Parameters
        ----------
        authority : str
            ABFS authority string in the form ``filesystem@account-host``.

        Returns
        -------
        tuple[str, str]
            A tuple containing the filesystem and account host.

        Raises
        ------
        ValueError
            If the authority string is not in the expected format.
        """
        if '@' not in authority:
            raise ValueError(
                'ABFS locations require authority in the form '
                '"filesystem@account.dfs.core.windows.net"',
            )
        file_system, account_host = authority.split('@', 1)
        if not file_system or not account_host:
            raise ValueError(
                'ABFS locations require authority in the form '
                '"filesystem@account.dfs.core.windows.net"',
            )
        return file_system, account_host

    # -- Instance Methods -- #

    def delete(
        self,
        location: StorageLocation,
    ) -> None:
        """
        Delete one ADLS Gen2 file if it exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.
        """
        self._validate(location)
        self._file_client(location).delete_file()

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """
        Return whether one ADLS Gen2 file exists.

        Parameters
        ----------
        location : StorageLocation
            Parsed storage location.

        Returns
        -------
        bool
            ``True`` when the file exists.
        """
        self._validate(location)
        return bool(self._file_client(location).exists())

    def open(
        self,
        location: StorageLocation,
        mode: str = 'r',
        **kwargs: Any,
    ) -> IO[Any]:
        """
        Open one ADLS Gen2 file via an in-memory file-like buffer.

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
            In-memory file-like object backed by ADLS download or upload calls.

        Raises
        ------
        TypeError
            If unsupported keyword arguments are provided.
        """
        self._validate(location)
        kind, text_mode = parse_remote_open_mode(mode)
        file_client = self._file_client(location)
        encoding = kwargs.pop('encoding', 'utf-8')
        errors = kwargs.pop('errors', None)
        newline = kwargs.pop('newline', None)
        overwrite = kwargs.pop('overwrite', True)
        content_type = kwargs.pop('content_type', None)
        if kwargs:
            unexpected = ', '.join(sorted(kwargs))
            raise TypeError(
                f'Unsupported ABFS open() keyword arguments: {unexpected}',
            )

        if kind == 'read':
            payload = file_client.download_file().readall()
            return open_remote_buffer(
                kind='read',
                text_mode=text_mode,
                payload=payload,
                encoding=encoding,
                errors=errors,
                newline=newline,
            )

        _, content_settings_type = _import_datalake_types()

        def _uploader(payload: bytes) -> None:
            upload_kwargs: dict[str, Any] = {
                'data': payload,
                'overwrite': overwrite,
            }
            if content_type is not None and content_settings_type is not None:
                upload_kwargs['content_settings'] = content_settings_type(
                    content_type=content_type,
                )
            file_client.upload_data(**upload_kwargs)

        return open_remote_buffer(
            kind='write',
            text_mode=text_mode,
            uploader=_uploader,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
