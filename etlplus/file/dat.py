"""
:mod:`etlplus.file.dat` module.

Helpers for reading/writing data (DAT) files.

Notes
-----
- A DAT file is a generic data file that may use various delimiters or fixed-
    width formats.
- Common cases:
    - Delimited text files (e.g., CSV, TSV).
    - Fixed-width formatted files.
    - Custom formats specific to certain applications.
- Rule of thumb:
    - If the file does not follow a specific standard format, use this module
        for reading and writing.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Protocol
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import write_delimited
from .base import DelimitedTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DatFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_DELIMITERS = ',\t|;'


# SECTION: INTERNAL PROTOCOLS =============================================== #


class _CsvSniffer(Protocol):
    """Protocol for objects providing ``csv.Sniffer``-compatible methods."""

    def has_header(
        self,
        sample: str,
    ) -> bool: ...

    def sniff(
        self,
        sample: str,
        delimiters: str | None = None,
    ) -> csv.Dialect: ...


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _sniff(
    sample: str,
    *,
    sniffer: _CsvSniffer | None = None,
    delimiters: str = _DEFAULT_DELIMITERS,
) -> tuple[csv.Dialect, bool]:
    """
    Infer CSV dialect and header presence from an input sample.

    Parameters
    ----------
    sample : str
        Initial bytes decoded as text from the file (e.g., first 4 KiB).
    sniffer : _CsvSniffer | None, optional
        Sniffer instance used to infer the dialect/header. When omitted, a
        default :class:`csv.Sniffer` instance is created.
    delimiters : str, optional
        Candidate delimiter characters passed to :meth:`csv.Sniffer.sniff`.

    Returns
    -------
    tuple[csv.Dialect, bool]
        ``(dialect, has_header)`` where *dialect* is the inferred CSV dialect
        and *has_header* indicates whether the first non-empty row is likely a
        header row.

    Notes
    -----
    - If dialect inference fails, the Excel dialect is used as a fallback.
    - If header detection fails, the function defaults to ``True`` so the
        first row is treated as a header.
    """
    sniffer_instance = sniffer or csv.Sniffer()
    try:
        dialect = cast(
            csv.Dialect,
            sniffer_instance.sniff(sample, delimiters=delimiters),
        )
    except csv.Error:
        dialect = cast(csv.Dialect, csv.get_dialect('excel'))
    try:
        has_header = sniffer_instance.has_header(sample)
    except csv.Error:
        has_header = True
    return dialect, has_header


# SECTION: CLASSES ========================================================== #


class DatFile(DelimitedTextFileHandlerABC):
    """
    Handler implementation for DAT files.

    DAT files are often delimited text, but the delimiter may vary between
    commas, tabs, pipes, semicolons, or other dialect variants.
    """

    # -- Class Attributes -- #

    format = FileFormat.DAT
    delimiter = ','

    # -- Instance Methods -- #

    def sniff(
        self,
        sample: str,
        *,
        sniffer: _CsvSniffer | None = None,
        delimiters: str = _DEFAULT_DELIMITERS,
    ) -> tuple[csv.Dialect, bool]:
        """
        Infer dialect/header for DAT payloads.

        Parameters
        ----------
        sample : str
            Initial bytes decoded as text from the DAT file.
        sniffer : _CsvSniffer | None, optional
            Optional custom sniffer implementation.
        delimiters : str, optional
            Candidate delimiters used during sniffing.

        Returns
        -------
        tuple[csv.Dialect, bool]
            Inferred dialect and whether the file has a header row.
        """
        return _sniff(sample, sniffer=sniffer, delimiters=delimiters)

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read DAT rows from *path*, sniffing dialect and header.

        Parameters
        ----------
        path : Path
            Path to the DAT file on disk.
        options : ReadOptions | None, optional
            Optional read parameters. Extra keys supported:
            - ``delimiters``: candidate delimiters string
            - ``sniffer``: custom sniffer object for deterministic behavior

        Returns
        -------
        JSONList
            The list of dictionaries read from the DAT file.
        """
        delimiters = _DEFAULT_DELIMITERS
        sniffer: _CsvSniffer | None = None
        extra_delimiters = self.read_extra_option(
            options,
            'delimiters',
            default=delimiters,
        )
        if extra_delimiters is not None:
            delimiters = str(extra_delimiters)
        extra_sniffer = self.read_extra_option(options, 'sniffer')
        if extra_sniffer is not None:
            sniffer = cast(_CsvSniffer, extra_sniffer)

        with path.open('r', encoding='utf-8', newline='') as handle:
            sample = handle.read(4096)
            handle.seek(0)
            dialect, has_header = self.sniff(
                sample,
                sniffer=sniffer,
                delimiters=delimiters,
            )
            reader = csv.reader(handle, dialect)
            rows = [
                row for row in reader if any(field.strip() for field in row)
            ]
            if not rows:
                return []

            if has_header:
                header = rows[0]
                data_rows = rows[1:]
            else:
                header = [f'col_{i + 1}' for i in range(len(rows[0]))]
                data_rows = rows

        records: JSONList = []
        for row in data_rows:
            record: JSONDict = {}
            for index, name in enumerate(header):
                record[name] = row[index] if index < len(row) else None
            records.append(record)
        return records

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write DAT rows to *path*.

        Parameters
        ----------
        path : Path
            Path to the DAT file on disk.
        rows : JSONList
            Rows to write.
        options : WriteOptions | None, optional
            Optional write parameters. Extra key ``delimiter`` can override
            :attr:`delimiter`.

        Returns
        -------
        int
            The number of rows written to the DAT file.
        """
        return write_delimited(
            path,
            rows,
            delimiter=self.delimiter_from_write_options(options),
            format_name='DAT',
        )


# SECTION: INTERNAL CONSTANTS =============================================== #


_DAT_HANDLER = DatFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``DatFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the DAT file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the DAT file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _DAT_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``DatFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the DAT file on disk.
    data : JSONData
        Data to write as DAT file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the DAT file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _DAT_HANDLER.write,
    )
