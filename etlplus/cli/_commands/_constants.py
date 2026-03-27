"""
:mod:`etlplus.cli._commands._constants` module.

Shared constants for :mod:`etlplus.cli`.
"""

from __future__ import annotations

from textwrap import dedent
from typing import Final

from ...connector import DataConnectorType
from ...file import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'CLI_DESCRIPTION',
    'CLI_EPILOG',
    'DATA_CONNECTORS',
    'DEFAULT_FILE_FORMAT',
    'FILE_FORMATS',
    'PROJECT_URL',
]


# SECTION: CONSTANTS ======================================================== #


DATA_CONNECTORS: Final[frozenset[str]] = frozenset(DataConnectorType.choices())

FILE_FORMATS: Final[frozenset[str]] = frozenset(FileFormat.choices())
DEFAULT_FILE_FORMAT: Final[str] = 'json'

CLI_DESCRIPTION: Final[str] = dedent(
    """
    ETLPlus - A Swiss Army knife for simple ETL operations.

    Provide a subcommand and options. Examples:

        etlplus extract in.csv > out.json
        etlplus validate in.json --rules "{"required": ["id"]}"
        etlplus transform in.json --source-type file out.json
        --target-type file --operations "{"select": ["id"]}"
        etlplus extract in.csv | etlplus load out.json --target-type file
        cat data.json | etlplus load https://example.com/data --target-type api

    Override format inference when extensions are misleading:

        etlplus extract data.txt --source-format csv
        etlplus load payload.bin --target-format json
    """,
).strip()
CLI_EPILOG: Final[str] = dedent(
    """
    Tip:
    `--source-format` and `--target-format` override format inference based on
    filename extensions when needed.
    """,
).strip()

PROJECT_URL: Final[str] = 'https://github.com/Dagitali/ETLPlus'
