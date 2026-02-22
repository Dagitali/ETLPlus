"""
:mod:`etlplus.file._stub_categories` module.

Internal category-specific placeholder handler ABCs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _stub_read(
    handler: StubFileHandlerABC,
    path: Path,
    *,
    options: ReadOptions | None = None,
) -> JSONData:
    """
    Delegate one read call to :class:`StubFileHandlerABC`.
    """
    return StubFileHandlerABC.read(handler, path, options=options)


def _stub_write(
    handler: StubFileHandlerABC,
    path: Path,
    data: JSONData,
    *,
    options: WriteOptions | None = None,
) -> int:
    """
    Delegate one write call to :class:`StubFileHandlerABC`.
    """
    return StubFileHandlerABC.write(handler, path, data, options=options)


# SECTION: CLASSES ========================================================== #


class StubBinarySerializationFileHandlerABC(
    BinarySerializationFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder binary-serialization handler contract.
    """

    # -- Instance Methods -- #

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Raise :class:`NotImplementedError` for binary payload writes.
        """
        _stub_write(self, self._stub_path(), data, options=options)
        return b''

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Raise :class:`NotImplementedError` for binary payload reads.
        """
        _ = payload
        return _stub_read(self, self._stub_path(), options=options)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for binary payload reads.
        """
        return cast(JSONList, _stub_read(self, path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for binary payload writes.
        """
        return _stub_write(self, path, data, options=options)


class StubEmbeddedDatabaseFileHandlerABC(
    EmbeddedDatabaseFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder embedded-database handler contract.
    """

    # -- Instance Methods -- #

    def connect(
        self,
        path: Path,
    ) -> Any:
        """
        Raise :class:`NotImplementedError` for connection creation.
        """
        return cast(Any, _stub_read(self, path))

    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        """
        Raise :class:`NotImplementedError` for table listing.
        """
        _ = connection
        return cast(
            list[str],
            _stub_read(self, self._stub_path()),
        )

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for embedded-database reads.
        """
        return cast(JSONList, _stub_read(self, path, options=options))

    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for table reads.
        """
        _ = connection
        _ = table
        return cast(JSONList, _stub_read(self, self._stub_path()))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for embedded-database writes.
        """
        return _stub_write(self, path, data, options=options)

    def write_table(
        self,
        connection: Any,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for table writes.
        """
        _ = connection
        _ = table
        return _stub_write(self, self._stub_path(), rows)


class StubLogEventFileHandlerABC(
    LogEventFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder log-event handler contract.
    """

    # -- Instance Methods -- #

    def parse_line(
        self,
        line: str,
    ) -> JSONDict:
        """
        Raise :class:`NotImplementedError` for line parsing.
        """
        _ = line
        return cast(JSONDict, _stub_read(self, self._stub_path()))

    def serialize_event(
        self,
        event: JSONDict,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for event serialization.
        """
        _stub_write(self, self._stub_path(), event)
        return ''


class StubSemiStructuredTextFileHandlerABC(
    SemiStructuredTextFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder semi-structured text handler contract.
    """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for semi-structured reads.
        """
        return cast(JSONList, _stub_read(self, path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for semi-structured writes.
        """
        return _stub_write(self, path, data, options=options)

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Raise :class:`NotImplementedError` for text payload parsing.
        """
        _ = text
        return _stub_read(self, self._stub_path(), options=options)

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for text payload serialization.
        """
        _stub_write(self, self._stub_path(), data, options=options)
        return ''


class StubSingleDatasetScientificFileHandlerABC(
    SingleDatasetScientificFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder single-dataset scientific handler contract.
    """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read content from *path* through the single-dataset contract.
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
        """
        self.resolve_single_read_dataset(
            dataset,
            options=options,
        )
        return cast(JSONList, _stub_read(self, path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to *path* through the single-dataset contract.
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
        """
        self.resolve_single_write_dataset(
            dataset,
            options=options,
        )
        return _stub_write(self, path, data, options=options)


class StubSpreadsheetFileHandlerABC(
    SpreadsheetFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder spreadsheet handler contract.
    """

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
        """
        return cast(JSONList, _stub_read(self, path, options=options))

    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Raise :class:`NotImplementedError` for sheet reads.
        """
        _ = sheet
        return cast(JSONList, _stub_read(self, path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Raise :class:`NotImplementedError` for spreadsheet writes.
        """
        return _stub_write(self, path, data, options=options)

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
        """
        _ = sheet
        return _stub_write(self, path, rows, options=options)


class StubTemplateFileHandlerABC(
    TemplateFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder template handler contract.
    """

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
        """
        _ = template
        _ = context
        return cast(str, _stub_read(self, self._stub_path()))
