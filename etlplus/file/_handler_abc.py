"""
:mod:`etlplus.file._handler_abc` module.

Shared abstract read/write skeletons for file handlers.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar

from ..types import JSONData
from ..types import JSONList
from ..utils import count_records
from ._io import EmbeddedDatabaseTableOption
from ._io import FileHandlerOption
from ._io import ScientificDatasetOption
from ._io import SpreadsheetSheetOption
from ._io import normalize_records
from ._io import read_text
from ._io import write_text
from ._sql import resolve_table

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: ABSTRACT BASE CLASSES ============================================ #


class BinarySerializationABC(ABC):
    """
    Shared path-level read/write flow for binary serialization handlers.
    """

    # -- Abstract Instance Methods -- #

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

    @abstractmethod
    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse binary payload bytes into structured data.

        Parameters
        ----------
        payload : bytes
            Binary payload to parse.
        options : ReadOptions | None, optional
            Read options to use when parsing the payload.
            Defaults to ``None``.

        Returns
        -------
        JSONData
            Structured data parsed from the binary payload.
        """

    # -- Instance Methods -- #

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return the default record count for binary write operations.

        Parameters
        ----------
        data : JSONData
            Structured data to count records for.

        Returns
        -------
        int
            Number of records in the structured data.
        """
        return count_records(data)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and decode binary serialization payload bytes from *path*.

        Parameters
        ----------
        path : Path
            Path to read the binary payload from.
        options : ReadOptions | None, optional
            Read options to use when parsing the payload.
            Defaults to ``None``.

        Returns
        -------
        JSONData
            Structured data parsed from the binary payload.
        """
        return self.loads_bytes(path.read_bytes(), options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Encode and write binary serialization payload bytes to *path*.

        Parameters
        ----------
        path : Path
            Path to write the binary payload to.
        data : JSONData
            Structured data to serialize and write.
        options : WriteOptions | None, optional
            Write options to use when encoding the payload.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        payload = self.dumps_bytes(data, options=options)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return self.count_written_records(data)


class ColumnarABC(ABC):
    """
    Shared read/write dispatch for columnar table handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_table(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> Any:
        """
        Read a columnar table object from *path*.

        Parameters
        ----------
        path : Path
            Path to read the columnar table from.
        options : ReadOptions | None, optional
            Read options to use when parsing the table.
            Defaults to ``None``.

        Returns
        -------
        Any
            Columnar table object read from *path*.
        """

    @abstractmethod
    def records_to_table(
        self,
        data: JSONData,
    ) -> Any:
        """
        Convert row-oriented records into a table object.

        Parameters
        ----------
        data : JSONData
            Row-oriented records to convert.

        Returns
        -------
        Any
            Columnar table object created from the records.
        """

    @abstractmethod
    def table_to_records(
        self,
        table: Any,
    ) -> JSONList:
        """
        Convert a table object into row-oriented records.

        Parameters
        ----------
        table : Any
            Columnar table object to convert.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the table.
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

        Parameters
        ----------
        path : Path
            Path to write the columnar table to.
        table : Any
            Columnar table object to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the table.
            Defaults to ``None``.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return columnar content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the columnar table from.
        options : ReadOptions | None, optional
            Read options to use when parsing the table.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the columnar table.
        """
        return self.table_to_records(self.read_table(path, options=options))

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write columnar content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the columnar table to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the table.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self.write_table(
            path,
            self.records_to_table(rows),
            options=options,
        )
        return len(rows)


class EmbeddedDatabaseABC(EmbeddedDatabaseTableOption, ABC):
    """
    Shared read/write dispatch for embedded-database handlers.
    """

    # -- Class Attributes -- #

    engine_name: ClassVar[str]
    default_table: ClassVar[str]
    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def connect(
        self,
        path: Path,
    ) -> Any:
        """
        Open and return a database connection for *path*.

        Parameters
        ----------
        path : Path
            Path to the embedded database file on disk.

        Returns
        -------
        Any
            Database connection object.
        """

    @abstractmethod
    def list_tables(
        self,
        connection: Any,
    ) -> list[str]:
        """
        Return readable table names from *connection*.

        Parameters
        ----------
        connection : Any
            Database connection object.

        Returns
        -------
        list[str]
            List of readable table names.
        """

    @abstractmethod
    def read_table(
        self,
        connection: Any,
        table: str,
    ) -> JSONList:
        """
        Read rows from *table*.

        Parameters
        ----------
        connection : Any
            Database connection object.
        table : str
            Name of the table to read.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the table.
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

        Parameters
        ----------
        connection : Any
            Database connection object.
        table : str
            Name of the table to write to.
        rows : JSONList
            Row-oriented records to write.

        Returns
        -------
        int
            Number of records written.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return embedded-database content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the embedded-database content from.
        options : ReadOptions | None, optional
            Read options to use when parsing the database.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the embedded-database.
        """
        connection = self.connect(path)
        try:
            table = self.table_from_read_options(options)
            if table is None:
                table = resolve_table(
                    self.list_tables(connection),
                    engine_name=self.engine_name,
                    default_table=self.default_table,
                )
                if table is None:
                    return []
            return self.read_table(connection, table)
        finally:
            self.close_connection(connection)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write embedded-database content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the embedded-database content to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the database.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.

        Raises
        ------
        ValueError
            If no table name is specified in options and no default table can
            be resolved from the database.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        table = self.table_from_write_options(
            options,
            default=self.default_table,
        )
        if table is None:  # pragma: no cover - guarded by default
            raise ValueError(f'{self.format_name} write requires a table name')
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect(path)
        try:
            return self.write_table(connection, table, rows)
        finally:
            self.close_connection(connection)


class RowReadWriteABC(ABC):
    """
    Shared read/write dispatch for row-oriented file handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read row records from *path*.

        Parameters
        ----------
        path : Path
            Path to read the row records from.
        options : ReadOptions | None, optional
            Read options to use when parsing the row records.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the file.
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
        Write row records to *path*.

        Parameters
        ----------
        path : Path
            Path to write the row records to.
        rows : JSONList
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the row records.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return row-oriented content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the row records from.
        options : ReadOptions | None, optional
            Read options to use when parsing the row records.
            Defaults to ``None``.

        Returns
        -------
        JSONList
            Row-oriented records extracted from the file.
        """
        return self.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write row-oriented content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the row records to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the row records.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        return self.write_rows(
            path,
            normalize_records(data, self.format_name),
            options=options,
        )


class SemiStructuredTextABC(FileHandlerOption, ABC):
    """
    Shared path-level read/write flow for semi-structured text handlers.
    """

    # -- Class Attributes -- #

    write_trailing_newline: ClassVar[bool] = False

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize structured data into text.
        """

    @abstractmethod
    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse *text* into structured data.
        """

    # -- Instance Methods -- #

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return the default record count for write operations.
        """
        return count_records(data)

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return semi-structured text content from *path*.

        Parameters
        ----------
        path : Path
            Path to read the row records from.
        options : ReadOptions | None, optional
            Read options to use when parsing the row records.
            Defaults to ``None``.

        Returns
        -------
        JSONData
            Semi-structured text content extracted from the file.
        """
        return self.loads(
            read_text(path, encoding=self.encoding_from_read_options(options)),
            options=options,
        )

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write semi-structured text content to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to write the row records to.
        data : JSONData
            Row-oriented records to write.
        options : WriteOptions | None, optional
            Write options to use when encoding the row records.
            Defaults to ``None``.

        Returns
        -------
        int
            Number of records written.
        """
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=self.encoding_from_write_options(options),
            trailing_newline=self.write_trailing_newline,
        )
        return self.count_written_records(data)


class ScientificDatasetABC(ScientificDatasetOption, ABC):
    """
    Shared read/write dispatch for scientific dataset handlers.
    """

    # -- Abstract Instance Methods -- #

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

        Parameters
        ----------
        path : Path
            The path to the file containing the dataset.
        dataset : str | None, optional
            The name of the dataset to read. If not provided, the default
            dataset will be used.
        options : ReadOptions | None, optional
            Additional options for reading the dataset.

        Returns
        -------
        JSONData
            The content of the dataset.

        Raises
        ------
        ValueError
            If the specified dataset does not exist.
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

        Parameters
        ----------
        path : Path
            The path to the file where the dataset will be written.
        data : JSONData
            The data to write to the dataset.
        dataset : str | None, optional
            The name of the dataset to write. If not provided, the default
            dataset will be used.
        options : WriteOptions | None, optional
            Additional options for writing the dataset.

        Returns
        -------
        int
            The number of records written to the dataset.
        """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return scientific dataset content from *path*.

        Parameters
        ----------
        path : Path
            The path to the file containing the dataset.
        options : ReadOptions | None, optional
            Additional options for reading the dataset.

        Returns
        -------
        JSONData
            The content of the dataset.
        """
        dataset = self.resolve_dataset(options=options)
        return self.read_dataset(path, dataset=dataset, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write scientific dataset content to *path* and return record count.

        Parameters
        ----------
        path : Path
            The path to the file where the dataset will be written.
        data : JSONData
            The data to write to the dataset.
        options : WriteOptions | None, optional
            Additional options for writing the dataset.

        Returns
        -------
        int
            The number of records written to the dataset.
        """
        dataset = self.resolve_dataset(options=options)
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )


class SpreadsheetSheetABC(SpreadsheetSheetOption, ABC):
    """
    Shared read/write dispatch for spreadsheet handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Abstract Instance Methods -- #

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

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return spreadsheet content from *path*.
        """
        sheet = self.sheet_from_options(options)
        return self.read_sheet(path, sheet=sheet, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write spreadsheet content to *path* and return record count.

        Parameters
        ----------
        path : Path
            The path to the file where the spreadsheet will be written.
        data : JSONData
            The data to write to the spreadsheet.
        options : WriteOptions | None, optional
            Additional options for writing the spreadsheet.

        Returns
        -------
        int
            The number of records written to the spreadsheet.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        sheet = self.sheet_from_options(options)
        path.parent.mkdir(parents=True, exist_ok=True)
        return self.write_sheet(path, rows, sheet=sheet, options=options)
