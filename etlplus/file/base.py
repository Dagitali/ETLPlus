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

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ReadOptions',
    'WriteOptions',
    # Base Classes
    'FileHandlerABC',
    'ReadOnlyFileHandlerABC',
    # Secondary / Category Base Classes
    'DelimitedTextFileHandlerABC',
    'TextFixedWidthFileHandlerABC',
    'SemiStructuredTextFileHandlerABC',
    'ColumnarFileHandlerABC',
    'BinarySerializationFileHandlerABC',
    'EmbeddedDatabaseFileHandlerABC',
    'SpreadsheetFileHandlerABC',
    'ReadOnlySpreadsheetFileHandlerABC',
    'ScientificDatasetFileHandlerABC',
    'SingleDatasetScientificFileHandlerABC',
    'ArchiveWrapperFileHandlerABC',
    'LogEventFileHandlerABC',
    'TemplateFileHandlerABC',
]


# SECTION: DATA CLASSES ===================================================== #


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


class FileHandlerABC(ABC):
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

    # -- Instance Methods -- #

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


class ArchiveWrapperFileHandlerABC(FileHandlerABC):
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

    def inner_name_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract archive member selector from read options.
        """
        if options is not None and options.inner_name is not None:
            return options.inner_name
        return default

    def inner_name_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract archive member selector from write options.
        """
        if options is not None and options.inner_name is not None:
            return options.inner_name
        return default


class BinarySerializationFileHandlerABC(FileHandlerABC):
    """
    Base contract for binary serialization/interchange formats.

    Typical formats: Avro, BSON, CBOR, MessagePack, PB.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'binary_serialization'

    # -- Instance Methods -- #

    @abstractmethod
    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse binary payload bytes into structured data.
        """

    @abstractmethod
    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize structured data into binary payload bytes.
        """


class ColumnarFileHandlerABC(FileHandlerABC):
    """
    Base contract for columnar analytics formats.

    Typical formats: Arrow, Feather, ORC, Parquet.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'columnar_analytics'
    engine_name: ClassVar[str]

    # -- Instance Methods -- #

    @abstractmethod
    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a columnar table object from *path*.
        """

    @abstractmethod
    def write_table(
        self,
        path: Path,
        table: Any,
        *,
        options: WriteOptions | None = None,
    ) -> None:
        """
        Write a columnar table object to *path*.
        """

    @abstractmethod
    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a table object into row-oriented records.
        """

    @abstractmethod
    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into a table object.
        """


class DelimitedTextFileHandlerABC(FileHandlerABC):
    """
    Base contract for delimited text formats.

    Typical formats: CSV, TSV, TAB, PSV, DAT.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'tabular_delimited_text'
    delimiter: ClassVar[str]
    quotechar: ClassVar[str] = '"'
    has_header: ClassVar[bool] = True

    # -- Instance Methods -- #

    @abstractmethod
    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read delimited rows from *path*.
        """

    @abstractmethod
    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write delimited *rows* to *path*.
        """


class TextFixedWidthFileHandlerABC(FileHandlerABC):
    """
    Base contract for plain text and fixed-width text formats.

    Typical formats: TXT, FWF.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'text_fixed_width'
    default_encoding: ClassVar[str] = 'utf-8'

    # -- Instance Methods -- #

    @abstractmethod
    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read text-backed rows from *path*.
        """

    @abstractmethod
    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write text-backed *rows* to *path*.
        """


class EmbeddedDatabaseFileHandlerABC(FileHandlerABC):
    """
    Base contract for file-backed embedded database formats.

    Typical formats: SQLite, DuckDB.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'embedded_database'
    default_table: ClassVar[str] = 'data'

    # -- Instance Methods -- #

    @abstractmethod
    def connect(
        self,
        path: Path,
    ) -> Any:
        """
        Open and return a database connection for *path*.
        """

    @abstractmethod
    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        """
        Return readable table names from *connection*.
        """

    @abstractmethod
    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        """
        Read rows from *table*.
        """

    @abstractmethod
    def write_table(
        self,
        connection: Any,
        table: str,
        rows: JSONList,
    ) -> int:
        """
        Write *rows* to *table*.
        """

    def table_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from read options.
        """
        if options is not None and options.table is not None:
            return options.table
        return default

    def table_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from write options.
        """
        if options is not None and options.table is not None:
            return options.table
        return default


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


class SemiStructuredTextFileHandlerABC(FileHandlerABC):
    """
    Base contract for semi-structured text formats.

    Typical formats: JSON, NDJSON, YAML, TOML, XML.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'semi_structured_text'
    allow_dict_root: ClassVar[bool] = True
    allow_list_root: ClassVar[bool] = True

    # -- Instance Methods -- #

    @abstractmethod
    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse *text* into structured JSON-like data.
        """

    @abstractmethod
    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* into a text payload.
        """


class ScientificDatasetFileHandlerABC(FileHandlerABC):
    """
    Base contract for scientific/statistical dataset containers.

    Typical formats: HDF5, NC, DTA, SAV, SAS7BDAT, XPT, RDA, RDS.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'scientific_dataset'
    dataset_key: ClassVar[str] = 'data'

    # -- Instance Methods -- #

    @abstractmethod
    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return available dataset keys within *path*.
        """

    @abstractmethod
    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return one dataset from *path*.
        """

    @abstractmethod
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

    def dataset_from_read_options(
        self,
        options: ReadOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from read options.
        """
        return options.dataset if options is not None else None

    def dataset_from_write_options(
        self,
        options: WriteOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from write options.
        """
        return options.dataset if options is not None else None


class SingleDatasetScientificFileHandlerABC(ScientificDatasetFileHandlerABC):
    """
    Base contract for scientific formats with a single dataset key.
    """

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return the single supported dataset key.
        """
        _ = path
        return [self.dataset_key]

    def validate_single_dataset_key(
        self,
        dataset: str | None,
    ) -> None:
        """
        Validate that *dataset* is either omitted or the default key.
        """
        if dataset is None or dataset == self.dataset_key:
            return
        raise ValueError(
            f'{self.format.value.upper()} supports only dataset key '
            f'{self.dataset_key!r}',
        )


class SpreadsheetFileHandlerABC(FileHandlerABC):
    """
    Base contract for spreadsheet formats.

    Typical formats: XLS, XLSX, XLSM, ODS.
    """

    # -- Class Attributes -- #

    category: ClassVar[str] = 'spreadsheet'
    engine_name: ClassVar[str]
    default_sheet: ClassVar[str | int] = 0

    # -- Instance Methods -- #

    @abstractmethod
    def read_sheet(
        self,
        path: Path,
        *,
        sheet: str | int,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read a single sheet from *path*.
        """

    @abstractmethod
    def write_sheet(
        self,
        path: Path,
        rows: JSONList,
        *,
        sheet: str | int,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write rows to a single sheet in *path*.
        """

    def sheet_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from read options.
        """
        if options is not None and options.sheet is not None:
            return options.sheet
        if default is not None:
            return default
        return self.default_sheet

    def sheet_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from write options.
        """
        if options is not None and options.sheet is not None:
            return options.sheet
        if default is not None:
            return default
        return self.default_sheet


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
