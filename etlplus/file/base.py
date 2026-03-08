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
from typing import Any
from typing import ClassVar

from ..utils.types import JSONData
from ..utils.types import JSONDict
from ..utils.types import JSONList
from ..utils.types import StrPath
from ._handler_abc import BinarySerializationABC
from ._handler_abc import ColumnarABC
from ._handler_abc import EmbeddedDatabaseABC
from ._handler_abc import RowReadWriteABC
from ._handler_abc import ScientificDatasetABC
from ._handler_abc import SemiStructuredTextABC
from ._handler_abc import SpreadsheetSheetABC
from ._io import ArchiveInnerNameOption
from ._io import DelimitedOption
from ._io import FileHandlerOption
from ._io import coerce_path
from ._io import read_delimited
from ._io import write_delimited
from ._mixins import SemiStructuredPayloadMixin
from ._mixins import SingleDatasetValidation
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
    'PlainTextFileHandlerABC',
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


# SECTION: TYPE ALIASES ===================================================== #


type SheetName = str | int
type SheetSelector = SheetName | None


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
    ) -> Any:
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
    sheet : SheetSelector
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
    sheet: SheetSelector = None
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
    sheet : SheetSelector
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
    sheet: SheetSelector = None
    table: str | None = None
    dataset: str | None = None
    inner_name: str | None = None
    extras: JSONDict = field(default_factory=dict)


# SECTION: ABSTRACT BASE CLASSES (PRIMARY) ================================== #


class FileHandlerABC(FileHandlerOption, ABC):
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
        return BoundFileHandler(self, coerce_path(path))

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
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
        Any
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
    ArchiveInnerNameOption,
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
    BinarySerializationABC,
    FileHandlerABC,
):
    """
    Base contract for binary serialization/interchange formats.

    Typical formats: Avro, BSON, CBOR, MessagePack, PB.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'binary_serialization'


class ColumnarFileHandlerABC(ColumnarABC, FileHandlerABC):
    """
    Base contract for columnar analytics formats.

    Typical formats: Arrow, Feather, ORC, Parquet.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'columnar_analytics'
    engine_name: ClassVar[str]


class DelimitedTextFileHandlerABC(
    RowReadWriteABC,
    DelimitedOption,
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
        Read delimited rows from *path* using :attr:`delimiter` or option
        overrides.

        Parameters
        ----------
        path : Path
            File path to read from.
        options : ReadOptions | None, optional
            Read options, which may include delimiter overrides. Defaults to
            ``None``.

        Returns
        -------
        JSONList
            List of parsed rows as dictionaries.
        """
        return read_delimited(
            path,
            delimiter=self.delimiter_from_options(options),
        )

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write delimited rows to *path* using :attr:`delimiter` or option
        overrides.

        Parameters
        ----------
        path : Path
            File path to write to.
        rows : JSONList
            List of row dictionaries to write.
        options : WriteOptions | None, optional
            Write options, which may include delimiter overrides. Defaults to
            ``None``.

        Returns
        -------
        int
            The number of rows written to the file.
        """
        return write_delimited(
            path,
            rows,
            delimiter=self.delimiter_from_options(options),
            format_name=self.format_name,
        )


class PlainTextFileHandlerABC(FileHandlerABC):
    """
    Base contract for plain text file handlers.

    Typical formats: TXT.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'plain_text'
    default_encoding: ClassVar[str] = 'utf-8'


class TextFixedWidthFileHandlerABC(RowReadWriteABC, FileHandlerABC):
    """
    Base contract for fixed-width row-oriented text formats.

    Typical formats: FWF.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'text_fixed_width'
    default_encoding: ClassVar[str] = 'utf-8'


class EmbeddedDatabaseFileHandlerABC(
    EmbeddedDatabaseABC,
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

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def parse_line(
        self,
        line: str,
    ) -> JSONDict:
        """
        Parse a single line into an event record.

        Parameters
        ----------
        line : str
            A single line from the log/event stream.

        Returns
        -------
        JSONDict
            The parsed event record.
        """

    @abstractmethod
    def serialize_event(
        self,
        event: JSONDict,
    ) -> str:
        """
        Serialize a single event record into one line.

        Parameters
        ----------
        event : JSONDict
            The event record to serialize.

        Returns
        -------
        str
            The serialized event record as a single line.
        """


class SemiStructuredTextFileHandlerABC(
    SemiStructuredTextABC,
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

        Parameters
        ----------
        text : str
            The raw text to parse.
        options : ReadOptions | None, optional
            Read options, which may include parsing overrides. Defaults to
            ``None``.

        Returns
        -------
        JSONData
            The parsed record payloads.
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

        Parameters
        ----------
        payload : JSONDict
            The dictionary payload to serialize.
        options : WriteOptions | None, optional
            Write options, which may include formatting overrides. Defaults to
            ``None``.

        Returns
        -------
        str
            The serialized dictionary payload as format-specific text.
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

        Parameters
        ----------
        data : JSONData
            The dictionary-root data to serialize.
        options : WriteOptions | None, optional
            Write options, which may include formatting overrides. Defaults to
            ``None``.

        Returns
        -------
        str
            The serialized dictionary-root data.
        """
        return self.dumps_dict_payload(
            self.require_dict_payload(data),
            options=options,
        )


class ScientificDatasetFileHandlerABC(
    ScientificDatasetABC,
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

        Parameters
        ----------
        path : Path
            File path to inspect for datasets.

        Returns
        -------
        list[str]
            List of dataset keys available within the file at *path*.
        """


class SingleDatasetScientificFileHandlerABC(
    SingleDatasetValidation,
    ScientificDatasetFileHandlerABC,
):
    """
    Base contract for scientific formats with a single dataset key.
    """


class SpreadsheetFileHandlerABC(SpreadsheetSheetABC, FileHandlerABC):
    """
    Base contract for spreadsheet formats.

    Typical formats: XLS, XLSX, XLSM, ODS.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'spreadsheet'
    engine_name: ClassVar[str]
    default_sheet: ClassVar[SheetName] = 0


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
        sheet: SheetName,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Reject sheet-level writes for read-only spreadsheet formats.

        Parameters
        ----------
        path : Path
            File path to write to.
        rows : JSONList
            Rows of data to write.
        sheet : SheetName
            Sheet name or index.
        options : WriteOptions | None, optional
            Write options, which may include formatting overrides. Defaults to
            ``None``.

        Returns
        -------
        int
            Never returns normally.
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
