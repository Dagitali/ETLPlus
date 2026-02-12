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


# SECTION: CLASSES ========================================================== #


class StubBinarySerializationFileHandlerABC(
    BinarySerializationFileHandlerABC,
    StubFileHandlerABC,
):
    """
    Placeholder binary-serialization handler contract.
    """

    # -- Instance Methods -- #

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
        return cast(
            JSONData,
            super().read(self._stub_path(), options=options),
        )

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Raise :class:`NotImplementedError` for binary payload writes.
        """
        super().write(self._stub_path(), data, options=options)
        return b''


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
        return cast(Any, StubFileHandlerABC.read(self, path))

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
            StubFileHandlerABC.read(self, self._stub_path()),
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
        return StubFileHandlerABC.read(self, path, options=options)

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
        return StubFileHandlerABC.read(self, self._stub_path())

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
        return StubFileHandlerABC.write(self, path, data, options=options)

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
        return StubFileHandlerABC.write(self, self._stub_path(), rows)


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
        return cast(JSONDict, super().read(self._stub_path()))

    def serialize_event(
        self,
        event: JSONDict,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for event serialization.
        """
        super().write(self._stub_path(), event)
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
        return StubFileHandlerABC.read(self, path, options=options)

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
        return StubFileHandlerABC.write(self, path, data, options=options)

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
        return cast(
            JSONData,
            StubFileHandlerABC.read(
                self,
                self._stub_path(),
                options=options,
            ),
        )

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Raise :class:`NotImplementedError` for text payload serialization.
        """
        StubFileHandlerABC.write(
            self,
            self._stub_path(),
            data,
            options=options,
        )
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
        dataset = self.resolve_read_dataset(options=options)
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
        dataset = self.resolve_read_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return StubFileHandlerABC.read(self, path, options=options)

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
        dataset = self.resolve_write_dataset(options=options)
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
        dataset = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(dataset)
        return StubFileHandlerABC.write(self, path, data, options=options)


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
        return StubFileHandlerABC.read(self, path, options=options)

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
        return StubFileHandlerABC.read(self, path, options=options)

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
        return StubFileHandlerABC.write(self, path, data, options=options)

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
        return StubFileHandlerABC.write(self, path, rows, options=options)


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
        return cast(str, super().read(self._stub_path()))
