"""
:mod:`etlplus.file._mixins` module.

Reusable mixins extracted from file handler ABCs.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ._io import coerce_record_payload as _coerce_record_payload
from ._io import normalize_records
from ._io import require_dict_payload as _require_dict_payload
from ._io import write_text

if TYPE_CHECKING:
    from .base import ReadOptions
    from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ArchiveInnerNameOptionMixin',
    'DelimitedOptionMixin',
    'EmbeddedDatabaseTableOptionMixin',
    'FileHandlerOptionMixin',
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


# SECTION: CLASSES (SECONDARY MIXINS) ======================================= #


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

    # -- Internal Static Methods -- #

    @staticmethod
    def _close_connection(
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
