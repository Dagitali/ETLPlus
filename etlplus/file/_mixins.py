"""
:mod:`etlplus.file._mixins` module.

Reusable mixins extracted from file handler ABCs.
"""

from __future__ import annotations

import re
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..utils import count_records
from ._io import coerce_record_payload as _coerce_record_payload
from ._io import normalize_records
from ._io import read_text
from ._io import require_dict_payload as _require_dict_payload
from ._io import stringify_value
from ._io import write_text
from ._sql import resolve_table

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ArchiveInnerNameOptionMixin',
    'DelimitedOptionMixin',
    'EmbeddedDatabaseTableOptionMixin',
    'FileHandlerOptionMixin',
    'BinarySerializationIOMixin',
    'ColumnarIOMixin',
    'EmbeddedDatabaseIOMixin',
    'RowReadWriteMixin',
    'ScientificDatasetIOMixin',
    'SemiStructuredTextIOMixin',
    'SingleDatasetValidationMixin',
    'SpreadsheetSheetIOMixin',
    'RegexTemplateRenderMixin',
    'ScientificDatasetOptionMixin',
    'SemiStructuredPayloadMixin',
    'SpreadsheetSheetOptionMixin',
    'TemplateTextIOMixin',
]


# SECTION: CLASSES (PRIMARY MIXINS) ========================================= #


class FileHandlerOptionMixin:
    """
    Shared helpers for common read/write option extraction.
    """

    # -- Internal Instance Methods -- #

    def _option_attr(
        self,
        options: ReadOptions | WriteOptions | None,
        attr_name: str,
    ) -> Any | None:
        """
        Return one option attribute value when present.

        Parameters
        ----------
        options : ReadOptions | WriteOptions | None
            Options to extract from, or ``None`` to skip.
        attr_name : str
            Name of the attribute to extract from *options* if present.

        Returns
        -------
        Any | None
            The value of the specified attribute on *options* if present, else
            ``None``.
        """
        if options is None:
            return None
        return getattr(options, attr_name)

    # -- Instance Methods -- #

    def encoding_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str = 'utf-8',
    ) -> str:
        """
        Extract text encoding from read options.

        Parameters
        ----------
        options : ReadOptions | None
            Options to extract from, or ``None`` to skip.
        default : str, optional
            Default encoding to use if not specified in options.

        Returns
        -------
        str
            The text encoding to use.
        """
        value = self._option_attr(options, 'encoding')
        if value is not None:
            return cast(str, value)
        return default

    def encoding_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str = 'utf-8',
    ) -> str:
        """
        Extract text encoding from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Options to extract from, or ``None`` to skip.
        default : str, optional
            Default encoding to use if not specified in options.

        Returns
        -------
        str
            The text encoding to use.
        """
        value = self._option_attr(options, 'encoding')
        if value is not None:
            return cast(str, value)
        return default

    def read_extra_option(
        self,
        options: ReadOptions | None,
        key: str,
        *,
        default: Any | None = None,
    ) -> Any | None:
        """
        Read one format-specific read option from ``options.extras``.

        Parameters
        ----------
        options : ReadOptions | None
            Options to extract from, or ``None`` to skip.
        key : str
            The key of the option to extract from *options.extras*.
        default : Any | None, optional
            Default value to return if the option is not present.

        Returns
        -------
        Any | None
            The value of the specified option if present, else *default*.
        """
        if options is None:
            return default
        return options.extras.get(key, default)

    def root_tag_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str = 'root',
    ) -> str:
        """
        Extract XML-like root tag from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Options to extract from, or ``None`` to skip.
        default : str, optional
            Default root tag to use if not specified in options.

        Returns
        -------
        str
            The root tag to use.
        """
        value = self._option_attr(options, 'root_tag')
        if value is not None:
            return cast(str, value)
        return default

    def write_extra_option(
        self,
        options: WriteOptions | None,
        key: str,
        *,
        default: Any | None = None,
    ) -> Any | None:
        """
        Read one format-specific write option from ``options.extras``.

        Parameters
        ----------
        options : WriteOptions | None
            Options to extract from, or ``None`` to skip.
        key : str
            The key of the option to extract from *options.extras*.
        default : Any | None, optional
            Default value to return if the option is not present.

        Returns
        -------
        Any | None
            The value of the specified option if present, else *default*.
        """
        if options is None:
            return default
        return options.extras.get(key, default)


# SECTION: CLASSES (IO MIXINS) ============================================== #


class BinarySerializationIOMixin(ABC):
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
        """

    # -- Instance Methods -- #

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return the default record count for binary write operations.
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
        """
        payload = self.dumps_bytes(data, options=options)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return self.count_written_records(data)


class ColumnarIOMixin(ABC):
    """
    Shared read/write dispatch for columnar table handlers.
    """

    format_name: str

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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return columnar content from *path*.
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


class EmbeddedDatabaseIOMixin(ABC):
    """
    Shared read/write dispatch for embedded-database handlers.
    """

    engine_name: ClassVar[str]
    default_table: ClassVar[str]
    format_name: str

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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return embedded-database content from *path*.
        """
        database_handler = cast(Any, self)
        connection = self.connect(path)
        try:
            table = database_handler.table_from_read_options(options)
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
            database_handler.close_connection(connection)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write embedded-database content to *path* and return record count.
        """
        database_handler = cast(Any, self)
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        table = database_handler.table_from_write_options(
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
            database_handler.close_connection(connection)


class RowReadWriteMixin(ABC):
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
        """
        return self.write_rows(
            path,
            normalize_records(data, self.format_name),
            options=options,
        )


class SemiStructuredTextIOMixin(FileHandlerOptionMixin, ABC):
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
        """
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=self.encoding_from_write_options(options),
            trailing_newline=self.write_trailing_newline,
        )
        return self.count_written_records(data)


# SECTION: CLASSES (OTHER MIXINS) =========================================== #


class ArchiveInnerNameOptionMixin(FileHandlerOptionMixin):
    """
    Shared helpers for archive member selection options.
    """

    # -- Instance Methods -- #

    def inner_name_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract archive member selector from read options.

        Parameters
        ----------
        options : ReadOptions | None
            Options to extract from, or ``None`` to skip.
        default : str | None, optional
            Default value to return if the option is not present.

        Returns
        -------
        str | None
            The archive member selector if present, else *default*.
        """
        value = self._option_attr(options, 'inner_name')
        if value is not None:
            return cast(str, value)
        return default

    def inner_name_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract archive member selector from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Options to extract from, or ``None`` to skip.
        default : str | None, optional
            Default value to return if the option is not present.

        Returns
        -------
        str | None
            The archive member selector if present, else *default*.
        """
        value = self._option_attr(options, 'inner_name')
        if value is not None:
            return cast(str, value)
        return default


class DelimitedOptionMixin(FileHandlerOptionMixin):
    """
    Shared helpers for delimiter overrides on delimited text handlers.
    """

    # -- Class Attributes -- #

    delimiter: ClassVar[str]

    # -- Instance Methods -- #

    def delimiter_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str:
        """
        Extract delimiter override from read options.

        Parameters
        ----------
        options : ReadOptions | None
            Options to extract from, or ``None`` to skip.
        default : str | None, optional
            Default delimiter to use if not specified in options.

        Returns
        -------
        str
            The delimiter to use.
        """
        override = self.read_extra_option(options, 'delimiter')
        if override is not None:
            return str(override)
        if default is not None:
            return default
        return self.delimiter

    def delimiter_from_write_options(
        self,
        options: WriteOptions | None,
        *,
        default: str | None = None,
    ) -> str:
        """
        Extract delimiter override from write options.

        Parameters
        ----------
        options : WriteOptions | None
            Options to extract from, or ``None`` to skip.
        default : str | None, optional
            Default delimiter to use if not specified in options.

        Returns
        -------
        str
            The delimiter to use.
        """
        override = self.write_extra_option(options, 'delimiter')
        if override is not None:
            return str(override)
        if default is not None:
            return default
        return self.delimiter


class EmbeddedDatabaseTableOptionMixin(FileHandlerOptionMixin):
    """
    Shared helpers for embedded-database table selection and cleanup.
    """

    # -- Instance Methods -- #

    def table_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | None = None,
    ) -> str | None:
        """
        Extract table selector from read options.
        """
        value = self._option_attr(options, 'table')
        if value is not None:
            return cast(str, value)
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
        value = self._option_attr(options, 'table')
        if value is not None:
            return cast(str, value)
        return default

    # -- Static Methods -- #

    @staticmethod
    def close_connection(
        connection: Any,
    ) -> None:
        """
        Close a database connection when it exposes a ``close`` method.
        """
        closer = getattr(connection, 'close', None)
        if callable(closer):
            closer()


class SemiStructuredPayloadMixin:
    """
    Shared payload coercion helpers for semi-structured text handlers.
    """

    # -- Class Attributes -- #

    format_name: str

    # -- Instance Methods -- #

    def coerce_dict_root_payload(
        self,
        payload: object,
        *,
        error_message: str | None = None,
    ) -> JSONDict:
        """
        Coerce ``payload`` to a dictionary or raise ``TypeError``.
        """
        if isinstance(payload, dict):
            return cast(JSONDict, payload)
        if error_message is None:
            error_message = f'{self.format_name} root must be a dict'
        raise TypeError(error_message)

    def coerce_record_payload(
        self,
        payload: Any,
    ) -> JSONData:
        """
        Coerce ``payload`` into object-or-object-list record form.
        """
        return _coerce_record_payload(payload, format_name=self.format_name)

    def require_dict_payload(
        self,
        data: JSONData,
    ) -> JSONDict:
        """
        Validate and return one dictionary payload.
        """
        return _require_dict_payload(data, format_name=self.format_name)


class ScientificDatasetOptionMixin(FileHandlerOptionMixin):
    """
    Shared helpers for scientific dataset selection options.
    """

    # -- Instance Methods -- #

    def dataset_from_read_options(
        self,
        options: ReadOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from read options.
        """
        value = self._option_attr(options, 'dataset')
        if value is not None:
            return cast(str, value)
        return None

    def dataset_from_write_options(
        self,
        options: WriteOptions | None,
    ) -> str | None:
        """
        Extract dataset selector from write options.
        """
        value = self._option_attr(options, 'dataset')
        if value is not None:
            return cast(str, value)
        return None

    def resolve_read_dataset(
        self,
        dataset: str | None = None,
        *,
        options: ReadOptions | None = None,
        default: str | None = None,
    ) -> str | None:
        """
        Resolve read-time dataset selection using explicit, options, then
        default.
        """
        if dataset is not None:
            return dataset
        from_options = self.dataset_from_read_options(options)
        if from_options is not None:
            return from_options
        return default

    def resolve_write_dataset(
        self,
        dataset: str | None = None,
        *,
        options: WriteOptions | None = None,
        default: str | None = None,
    ) -> str | None:
        """
        Resolve write-time dataset selection using explicit, options, then
        default.
        """
        if dataset is not None:
            return dataset
        from_options = self.dataset_from_write_options(options)
        if from_options is not None:
            return from_options
        return default


class SpreadsheetSheetOptionMixin(FileHandlerOptionMixin):
    """
    Shared helpers for spreadsheet sheet-selection options.
    """

    # -- Class Attributes -- #

    default_sheet: ClassVar[str | int]

    # -- Instance Methods -- #

    def sheet_from_read_options(
        self,
        options: ReadOptions | None,
        *,
        default: str | int | None = None,
    ) -> str | int:
        """
        Extract sheet selector from read options.
        """
        value = self._option_attr(options, 'sheet')
        if value is not None:
            return cast(str | int, value)
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
        value = self._option_attr(options, 'sheet')
        if value is not None:
            return cast(str | int, value)
        if default is not None:
            return default
        return self.default_sheet


class ScientificDatasetIOMixin(ScientificDatasetOptionMixin, ABC):
    """
    Shared read/write dispatch for scientific dataset handlers.
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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return scientific dataset content from *path*.
        """
        dataset = self.dataset_from_read_options(options)
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
        """
        dataset = self.dataset_from_write_options(options)
        return self.write_dataset(
            path,
            data,
            dataset=dataset,
            options=options,
        )


class SingleDatasetValidationMixin(ScientificDatasetOptionMixin):
    """
    Shared helpers for single-dataset scientific handler variants.
    """

    dataset_key: ClassVar[str]
    format_name: str

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:
        """
        Return the single supported dataset key.
        """
        _ = path
        return [self.dataset_key]

    def resolve_single_read_dataset(
        self,
        dataset: str | None = None,
        *,
        options: ReadOptions | None = None,
    ) -> str | None:
        """
        Resolve and validate single-dataset read selection.
        """
        resolved = self.resolve_read_dataset(dataset, options=options)
        self.validate_single_dataset_key(resolved)
        return resolved

    def resolve_single_write_dataset(
        self,
        dataset: str | None = None,
        *,
        options: WriteOptions | None = None,
    ) -> str | None:
        """
        Resolve and validate single-dataset write selection.
        """
        resolved = self.resolve_write_dataset(dataset, options=options)
        self.validate_single_dataset_key(resolved)
        return resolved

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
            f'{self.format_name} supports only dataset key '
            f'{self.dataset_key!r}',
        )


class SpreadsheetSheetIOMixin(SpreadsheetSheetOptionMixin, ABC):
    """
    Shared read/write dispatch for spreadsheet handlers.
    """

    format_name: str

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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return spreadsheet content from *path*.
        """
        sheet = self.sheet_from_read_options(options)
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
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        sheet = self.sheet_from_write_options(options)
        path.parent.mkdir(parents=True, exist_ok=True)
        return self.write_sheet(path, rows, sheet=sheet, options=options)


class TemplateTextIOMixin:
    """
    Shared template-file read/write implementation.
    """

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return a single template row from *path*.

        Parameters
        ----------
        path : Path
            Path to read from.
        options : ReadOptions | None, optional
            Read options, which may include encoding overrides. Defaults to
            ``None``.

        Returns
        -------
        JSONList
            List containing one dictionary with the template key and text
            value.
        """
        template_handler = cast(Any, self)
        return [
            {
                template_handler.template_key: path.read_text(
                    encoding=template_handler.encoding_from_read_options(
                        options,
                    ),
                ),
            },
        ]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write one template row to *path* and return row count.

        Parameters
        ----------
        path : Path
            Path to write to.
        data : JSONData
            Data to write.
        options : WriteOptions | None, optional
            Write options, which may include encoding overrides. Defaults to
            ``None``.

        Returns
        -------
        int
            Number of rows written (0 or 1).

        Raises
        ------
        TypeError
             If *data* is not a one-item list of dictionaries with a string
                value for the template key.
        """
        template_handler = cast(Any, self)
        rows = normalize_records(data, template_handler.format_name)
        if not rows:
            return 0
        if len(rows) != 1:
            raise TypeError(
                f'{template_handler.format_name} payloads must contain '
                'exactly one object',
            )
        template_value = rows[0].get(template_handler.template_key)
        if not isinstance(template_value, str):
            raise TypeError(
                f'{template_handler.format_name} payloads must include a '
                f'"{template_handler.template_key}" string',
            )
        write_text(
            path,
            template_value,
            encoding=template_handler.encoding_from_write_options(options),
        )
        return 1


class RegexTemplateRenderMixin:
    """
    Shared regex-token template rendering implementation.
    """

    token_pattern: ClassVar[re.Pattern[str]]

    def template_key_from_match(
        self,
        match: re.Match[str],
    ) -> str | None:
        """
        Resolve one context key from a regex token match.
        """
        return cast(str | None, match.groupdict().get('key'))

    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Render template text by replacing regex token matches with context
        values.
        """

        def _replace(match: re.Match[str]) -> str:
            key = self.template_key_from_match(match)
            return stringify_value(context.get(key)) if key is not None else ''

        return self.token_pattern.sub(_replace, template)
