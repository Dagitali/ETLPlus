"""
:mod:`etlplus.file.base` module.

Abstract base classes for file-format handlers.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import ClassVar

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._io import read_delimited
from ._io import write_delimited
from ._mixins import ArchiveInnerNameOptionMixin
from ._mixins import BinarySerializationIOMixin
from ._mixins import ColumnarIOMixin
from ._mixins import DelimitedOptionMixin
from ._mixins import EmbeddedDatabaseIOMixin
from ._mixins import EmbeddedDatabaseTableOptionMixin
from ._mixins import FileHandlerOptionMixin
from ._mixins import RowReadWriteMixin
from ._mixins import ScientificDatasetIOMixin
from ._mixins import SemiStructuredPayloadMixin
from ._mixins import SemiStructuredTextIOMixin
from ._mixins import SingleDatasetValidationMixin
from ._mixins import SpreadsheetSheetIOMixin
from ._mixins import TemplateTextIOMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'BoundFileHandler',
    'ReadOptions',
    'WriteOptions',
    # Base Classes
    'FileHandlerABC',
    'ReadOnlyFileHandlerABC',
    # Secondary / Category Base Classes
    'ArchiveWrapperFileHandlerABC',
    'BinarySerializationFileHandlerABC',
    'ColumnarFileHandlerABC',
    'DelimitedTextFileHandlerABC',
    'DictPayloadSemiStructuredTextFileHandlerABC',
    'EmbeddedDatabaseFileHandlerABC',
    'LogEventFileHandlerABC',
    'RecordPayloadSemiStructuredTextFileHandlerABC',
    'ReadOnlySpreadsheetFileHandlerABC',
    'ScientificDatasetFileHandlerABC',
    'SemiStructuredTextFileHandlerABC',
    'SingleDatasetScientificFileHandlerABC',
    'SpreadsheetFileHandlerABC',
    'StandardDelimitedTextFileHandlerABC',
    'TemplateFileHandlerABC',
    'TemplateTextIOMixin',
    'TextFixedWidthFileHandlerABC',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, frozen=True)
class BoundFileHandler:
    """
    Path-bound facade around a format handler.

    Attributes
    ----------
    handler : FileHandlerABC
        Concrete format handler instance.
    path : Path
        Bound file path used by :meth:`read` and :meth:`write`.
    """

    handler: FileHandlerABC
    path: Path

    def read(
        self,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read from :attr:`path` using :attr:`handler`.
        """
        return self.handler.read(self.path, options=options)

    def write(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to :attr:`path` using :attr:`handler`.
        """
        return self.handler.write(self.path, data, options=options)


@dataclass(slots=True, frozen=True)
class ReadOptions:
    """
    Common optional parameters used when reading files.

    Attributes
    ----------
    encoding : str
        Text encoding used for text-backed formats.
    sheet : str | int | None
        Spreadsheet sheet selector (name or index).
    table : str | None
        Table name for embedded database file formats.
    dataset : str | None
        Dataset/group key for scientific container formats.
    inner_name : str | None
        Inner member name for archive-backed formats.
    extras : JSONDict
        Format-specific extra read options.
    """

    encoding: str = 'utf-8'
    sheet: str | int | None = None
    table: str | None = None
    dataset: str | None = None
    inner_name: str | None = None
    extras: JSONDict = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class WriteOptions:
    """
    Common optional parameters used when writing files.

    Attributes
    ----------
    encoding : str
        Text encoding used for text-backed formats.
    root_tag : str
        Root XML tag name for XML-like structured outputs.
    sheet : str | int | None
        Spreadsheet sheet selector (name or index).
    table : str | None
        Table name for embedded database file formats.
    dataset : str | None
        Dataset/group key for scientific container formats.
    inner_name : str | None
        Inner member name for archive-backed formats.
    extras : JSONDict
        Format-specific extra write options.
    """

    encoding: str = 'utf-8'
    root_tag: str = 'root'
    sheet: str | int | None = None
    table: str | None = None
    dataset: str | None = None
    inner_name: str | None = None
    extras: JSONDict = field(default_factory=dict)


# SECTION: ABSTRACT BASE CLASSES (PRIMARY) ================================== #


class FileHandlerABC(FileHandlerOptionMixin, ABC):
    """
    Root interface for format-specific file handlers.

    Subclasses should define :attr:`format` and implement :meth:`read` and
    :meth:`write`.
    """

    # -- Class Attributes -- #

    format: ClassVar[FileFormat]
    category: ClassVar[str] = 'generic'
    supports_read: ClassVar[bool] = True
    supports_write: ClassVar[bool] = True

    # -- Instance Properties -- #

    @property
    def format_name(
        self,
    ) -> str:
        """
        Return the normalized human-readable format name.

        Returns
        -------
        str
            Uppercase enum value for :attr:`format`.
        """
        return self.format.value.upper()

    # -- Instance Methods -- #

    def at(
        self,
        path: StrPath,
    ) -> BoundFileHandler:
        """
        Return a path-bound facade for this handler.

        Parameters
        ----------
        path : StrPath
            File path to bind to this handler.

        Returns
        -------
        BoundFileHandler
            Facade exposing ``read()`` and ``write(data)`` without a *path*
            argument.
        """
        return BoundFileHandler(self, Path(path))

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return data from *path*.

        Parameters
        ----------
        path : Path
            File path to read from.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """

    @abstractmethod
    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to *path* and return record count.

        Parameters
        ----------
        path : Path
            File path to write to.
        data : JSONData
            Payload to serialize and write.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of records written.
        """


class ReadOnlyFileHandlerABC(FileHandlerABC):
    """
    Base class for formats that support reads but not writes.
    """

    # -- Class Attributes -- #

    supports_write: ClassVar[bool] = False

    # -- Instance Methods -- #

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Reject writes for read-only formats.

        Parameters
        ----------
        path : Path
            File path to write to.
        data : JSONData
            Payload that would be written.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Never returns normally.

        Raises
        ------
        RuntimeError
            If write is attempted on a read-only format.
        """
        raise RuntimeError(
            f'{self.format.value.upper()} is read-only and does not support '
            'write operations',
        )


# SECTION: ABSTRACT BASE CLASSES (SECONDARY) ================================ #


class ArchiveWrapperFileHandlerABC(
    ArchiveInnerNameOptionMixin,
    FileHandlerABC,
):
    """
    Base contract for archive/compression wrapper formats.

    Typical formats: GZ, ZIP.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'archive_wrapper'
    default_inner_name: ClassVar[str] = 'data'

    # -- Instance Methods -- #

    @abstractmethod
    def read_inner_bytes(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> bytes:
        """
        Read inner member bytes from an archive at *path*.
        """

    @abstractmethod
    def write_inner_bytes(
        self,
        path: Path,
        payload: bytes,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write inner member bytes to an archive at *path*.
        """


class BinarySerializationFileHandlerABC(
    BinarySerializationIOMixin,
    FileHandlerABC,
):
    """
    Base contract for binary serialization/interchange formats.

    Typical formats: Avro, BSON, CBOR, MessagePack, PB.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'binary_serialization'


class ColumnarFileHandlerABC(ColumnarIOMixin, FileHandlerABC):
    """
    Base contract for columnar analytics formats.

    Typical formats: Arrow, Feather, ORC, Parquet.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'columnar_analytics'
    engine_name: ClassVar[str]


class DelimitedTextFileHandlerABC(
    RowReadWriteMixin,
    DelimitedOptionMixin,
    FileHandlerABC,
):
    """
    Base contract for delimited text formats.

    Typical formats: CSV, TSV, TAB, PSV, DAT.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'tabular_delimited_text'
    delimiter: ClassVar[str]
    quotechar: ClassVar[str] = '"'
    has_header: ClassVar[bool] = True


class StandardDelimitedTextFileHandlerABC(DelimitedTextFileHandlerABC):
    """
    Shared implementation for straightforward delimited text handlers.

    Subclasses only need to define :attr:`format` and :attr:`delimiter`.
    """

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited rows using :attr:`delimiter` or option overrides.
        """
        return read_delimited(
            path,
            delimiter=self.delimiter_from_read_options(options),
        )

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write delimited rows using :attr:`delimiter` or option overrides.
        """
        return write_delimited(
            path,
            rows,
            delimiter=self.delimiter_from_write_options(options),
            format_name=self.format_name,
        )


class TextFixedWidthFileHandlerABC(RowReadWriteMixin, FileHandlerABC):
    """
    Base contract for plain text and fixed-width text formats.

    Typical formats: TXT, FWF.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'text_fixed_width'
    default_encoding: ClassVar[str] = 'utf-8'


class EmbeddedDatabaseFileHandlerABC(
    EmbeddedDatabaseIOMixin,
    EmbeddedDatabaseTableOptionMixin,
    FileHandlerABC,
):
    """
    Base contract for file-backed embedded database formats.

    Typical formats: SQLite, DuckDB.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'embedded_database'
    engine_name: ClassVar[str] = 'database'
    default_table: ClassVar[str] = 'data'


class LogEventFileHandlerABC(FileHandlerABC):
    """
    Base contract for log/event stream formats.

    Typical formats: LOG, EVT, EVTX, W3CLOG.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'log_event_stream'
    line_oriented: ClassVar[bool] = True

    # -- Instance Methods -- #

    @abstractmethod
    def parse_line(
        self,
        line: str,
    ) -> JSONDict:
        """
        Parse a single line into an event record.
        """

    @abstractmethod
    def serialize_event(
        self,
        event: JSONDict,
    ) -> str:
        """
        Serialize a single event record into one line.
        """


class SemiStructuredTextFileHandlerABC(
    SemiStructuredTextIOMixin,
    SemiStructuredPayloadMixin,
    FileHandlerABC,
):
    """
    Base contract for semi-structured text formats.

    Typical formats: JSON, NDJSON, YAML, TOML, XML.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'semi_structured_text'
    allow_dict_root: ClassVar[bool] = True
    allow_list_root: ClassVar[bool] = True
    write_trailing_newline: ClassVar[bool] = False


class RecordPayloadSemiStructuredTextFileHandlerABC(
    SemiStructuredTextFileHandlerABC,
):
    """
    Shared base for semi-structured formats with record-like roots.
    """

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def loads_payload(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> object:
        """
        Parse raw text into a Python payload prior to record coercion.
        """

    # -- Instance Methods -- #

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse text into object-or-object-list record payloads.
        """
        return self.coerce_record_payload(
            self.loads_payload(text, options=options),
        )


class DictPayloadSemiStructuredTextFileHandlerABC(
    SemiStructuredTextFileHandlerABC,
):
    """
    Shared base for semi-structured formats that write dictionary payloads.
    """

    # -- Class Attributes -- #

    allow_dict_root: ClassVar[bool] = True
    allow_list_root: ClassVar[bool] = False

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def dumps_dict_payload(
        self,
        payload: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize one dictionary payload into format-specific text.
        """

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary-root data into format-specific text.
        """
        return self.dumps_dict_payload(
            self.require_dict_payload(data),
            options=options,
        )


class ScientificDatasetFileHandlerABC(
    ScientificDatasetIOMixin,
    FileHandlerABC,
):
    """
    Base contract for scientific/statistical dataset containers.

    Typical formats: HDF5, NC, DTA, SAV, SAS7BDAT, XPT, RDA, RDS.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'scientific_dataset'
    dataset_key: ClassVar[str] = 'data'

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available dataset keys within *path*.
        """


class SingleDatasetScientificFileHandlerABC(
    SingleDatasetValidationMixin,
    ScientificDatasetFileHandlerABC,
):
    """
    Base contract for scientific formats with a single dataset key.
    """


class SpreadsheetFileHandlerABC(SpreadsheetSheetIOMixin, FileHandlerABC):
    """
    Base contract for spreadsheet formats.

    Typical formats: XLS, XLSX, XLSM, ODS.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'spreadsheet'
    engine_name: ClassVar[str]
    default_sheet: ClassVar[str | int] = 0


class ReadOnlySpreadsheetFileHandlerABC(
    ReadOnlyFileHandlerABC,
    SpreadsheetFileHandlerABC,
):
    """
    Base contract for read-only spreadsheet formats.
    """

    # -- Instance Methods -- #

    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Reject sheet-level writes for read-only spreadsheet formats.
        """
        _ = sheet
        return ReadOnlyFileHandlerABC.write(
            self,
            path,
            rows,
            options=options,
        )


class TemplateFileHandlerABC(FileHandlerABC):
    """
    Base contract for template formats.

    Typical formats: Jinja2, Mustache, Handlebars, Velocity.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'template'
    template_engine: ClassVar[str]
    template_key: ClassVar[str] = 'template'

    # -- Instance Methods -- #

    @abstractmethod
    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Render *template* text using *context*.

        Parameters
        ----------
        template : str
            Template text to render.
        context : JSONDict
            Context data for rendering.

        Returns
        -------
        str
            Rendered template text.
        """
