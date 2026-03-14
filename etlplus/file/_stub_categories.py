"""
:mod:`etlplus.file._stub_categories` module.

Internal category-specific placeholder handler ABCs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import cast

from ..utils.types import JSONData
from ..utils.types import JSONDict
from ..utils.types import JSONList
from .base import BinarySerializationFileHandlerABC
from .base import EmbeddedDatabaseFileHandlerABC
from .base import LogEventFileHandlerABC
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import SingleDatasetScientificFileHandlerABC
from .base import SpreadsheetFileHandlerABC
from .base import TemplateFileHandlerABC
from .base import WriteOptions
from .stub import StubFileHandlerABC

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StubBinarySerializationFileHandlerABC',
    'StubEmbeddedDatabaseFileHandlerABC',
    'StubLogEventFileHandlerABC',
    'StubSemiStructuredTextFileHandlerABC',
    'StubSingleDatasetScientificFileHandlerABC',
    'StubSpreadsheetFileHandlerABC',
    'StubTemplateFileHandlerABC',
]


# SECTION: INTERNAL CLASSES ================================================= #


class _StubDelegationMixin(StubFileHandlerABC):
    """Shared delegation helpers for category-specific stub handlers."""

    def _read_path(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Delegate one path-based read call to :class:`StubFileHandlerABC`.

        Parameters
        ----------
        path : Path
            The file path to read from.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONData
            The resulting data from the delegated read.
        """
        return StubFileHandlerABC.read(self, path, options=options)

    def _read_stub_path(
        self,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Delegate one read call using :meth:`StubFileHandlerABC._stub_path`.

        Parameters
        ----------
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONData
            The resulting data from the delegated read.
        """
        return self._read_path(self._stub_path(), options=options)

    def _write_path(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Delegate one path-based write call to :class:`StubFileHandlerABC`.

        Parameters
        ----------
        path : Path
            The file path to write to.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return StubFileHandlerABC.write(self, path, data, options=options)

    def _write_stub_path(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Delegate one write call using :meth:`StubFileHandlerABC._stub_path`.

        Parameters
        ----------
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return self._write_path(self._stub_path(), data, options=options)


# SECTION: CLASSES ========================================================== #


class StubBinarySerializationFileHandlerABC(
    BinarySerializationFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder binary-serialization handler contract."""

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Raise :class:`NotImplementedError` for binary payload writes.

        Parameters
        ----------
        data : JSONData
            The structured data to serialize as bytes.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        bytes
            The resulting bytes from the attempted serialization.
        """
        self._write_stub_path(data, options=options)
        return b''

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Raise :class:`NotImplementedError` for binary payload reads.

        Parameters
        ----------
        payload : bytes
            The binary payload to deserialize.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONData
            The resulting data from the attempted deserialization.
        """
        _ = payload
        return self._read_stub_path(options=options)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for binary payload reads.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        return cast(JSONList, self._read_path(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for binary payload writes.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return self._write_path(path, data, options=options)


class StubEmbeddedDatabaseFileHandlerABC(
    EmbeddedDatabaseFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder embedded-database handler contract."""

    # -- Instance Methods -- #

    def connect(
        self,
        path: Path,
    ) -> Any:
        """
        Raise :class:`NotImplementedError` for connection creation.

        Parameters
        ----------
        path : Path
            Path to the embedded database file.

        Returns
        -------
        Any
            The resulting connection object from the attempted connection.
        """
        return cast(Any, self._read_path(path))

    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        """
        Raise :class:`NotImplementedError` for table listing.

        Parameters
        ----------
        connection : Any
            The embedded database connection object.

        Returns
        -------
        list[str]
            The resulting list of table names from the attempted listing.
        """
        _ = connection
        return cast(list[str], self._read_stub_path())

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for embedded-database reads.

        Parameters
        ----------
        path : Path
            Path to the embedded database file.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        return cast(JSONList, self._read_path(path, options=options))

    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for table reads.

        Parameters
        ----------
        connection : Any
            The embedded database connection object.
        table : str
            The name of the table to read from.

        Returns
        -------
        JSONList
            The resulting data from the attempted table read.
        """
        _ = connection
        _ = table
        return cast(JSONList, self._read_stub_path())

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for embedded-database writes.

        Parameters
        ----------
        path : Path
            Path to the embedded database file.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return self._write_path(path, data, options=options)

    def write_table(
        self,
        connection: Any,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for table writes.

        Parameters
        ----------
        connection : Any
            The embedded database connection object.
        table : str
            The name of the table to write to.
        rows : JSONList
            The rows to write.

        Returns
        -------
        int
            The number of bytes written.
        """
        _ = connection
        _ = table
        return self._write_stub_path(rows)


class StubLogEventFileHandlerABC(
    LogEventFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder log-event handler contract."""

    # -- Instance Methods -- #

    def parse_line(
        self,
        line: str,
    ) -> JSONDict:
        """
        Raise :class:`NotImplementedError` for line parsing.

        Parameters
        ----------
        line : str
            The log line to parse.

        Returns
        -------
        JSONDict
            The resulting event data from the attempted parsing.
        """
        _ = line
        return cast(JSONDict, self._read_stub_path())

    def serialize_event(
        self,
        event: JSONDict,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for event serialization.

        Parameters
        ----------
        event : JSONDict
            The event data to serialize.

        Returns
        -------
        str
            The resulting serialized event data.
        """
        self._write_stub_path(event)
        return ''


class StubSemiStructuredTextFileHandlerABC(
    SemiStructuredTextFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder semi-structured text handler contract."""

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for semi-structured reads.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        return cast(JSONList, self._read_path(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for semi-structured writes.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return self._write_path(path, data, options=options)

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Raise :class:`NotImplementedError` for text payload parsing.

        Parameters
        ----------
        text : str
            The text to decode.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONData
            The resulting data from the attempted parsing.
        """
        _ = text
        return self._read_stub_path(options=options)

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for text payload serialization.

        Parameters
        ----------
        data : JSONData
            The data to serialize.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        str
            The resulting serialized event data.
        """
        self._write_stub_path(data, options=options)
        return ''


class StubSingleDatasetScientificFileHandlerABC(
    SingleDatasetScientificFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder single-dataset scientific handler contract."""

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read content from *path* through the single-dataset contract.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        dataset = self.resolve_dataset(options=options)
        return self.read_dataset(path, dataset=dataset, options=options)

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return one dataset from *path*.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        dataset : str | None
            The dataset to read.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        self.resolve_single_dataset(dataset, options=options)
        return cast(JSONList, self._read_path(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to *path* through the single-dataset contract.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        dataset = self.resolve_dataset(options=options)
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one dataset to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        data : JSONData
            The data to write.
        dataset : str | None
            The dataset to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        self.resolve_single_dataset(dataset, options=options)
        return self._write_path(path, data, options=options)


class StubSpreadsheetFileHandlerABC(
    SpreadsheetFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder spreadsheet handler contract."""

    # -- Class Attributes -- #

    engine_name: ClassVar[str] = 'stub'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for spreadsheet reads.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        return cast(JSONList, self._read_path(path, options=options))

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for sheet reads.

        Parameters
        ----------
        path : Path
            Path to the file to read from.
        sheet : str | int
            The sheet to read.
        options : ReadOptions | None
            Optional read options.

        Returns
        -------
        JSONList
            The resulting data from the attempted read.
        """
        _ = sheet
        return cast(JSONList, self._read_path(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for spreadsheet writes.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        data : JSONData
            The data to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        return self._write_path(path, data, options=options)

    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for sheet writes.

        Parameters
        ----------
        path : Path
            Path to the file to write to.
        rows : JSONList
            The rows to write.
        sheet : str | int
            The sheet to write.
        options : WriteOptions | None
            Optional write options.

        Returns
        -------
        int
            The number of bytes written.
        """
        _ = sheet
        return self._write_path(path, rows, options=options)


class StubTemplateFileHandlerABC(
    TemplateFileHandlerABC,
    _StubDelegationMixin,
):
    """Placeholder template handler contract."""

    # -- Class Attributes -- #

    template_engine: ClassVar[str] = 'stub'

    # -- Instance Methods -- #

    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for template rendering.

        Parameters
        ----------
        template : str
            The template string to render.
        context : JSONDict
            The context data to render with the template.

        Returns
        -------
        str
            The resulting rendered template string.

        """
        _ = template
        _ = context
        return cast(str, self._read_stub_path())
