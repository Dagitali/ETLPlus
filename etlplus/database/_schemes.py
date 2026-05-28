"""
:mod:`etlplus.database._schemes` module.

Database URL and DSN helpers.
"""

from __future__ import annotations

from typing import Final

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DATABASE_SCHEMES',
    # Functions
    'is_database_dsn',
    'is_database_url',
]


# SECTION: CONSTANTS ======================================================== #


DATABASE_SCHEMES: Final[tuple[str, ...]] = (
    'bigquery://',
    'bigquery+',
    'duckdb://',
    'duckdb+',
    'mssql://',
    'mssql+',
    'mysql://',
    'mysql+',
    'oracle://',
    'oracle+',
    'postgres://',
    'postgres+',
    'postgresql://',
    'postgresql+',
    'snowflake://',
    'snowflake+',
    'sqlite://',
    'sqlite+',
)


# SECTION: FUNCTIONS ======================================================== #


def is_database_dsn(
    value: str,
) -> bool:
    """
    Return whether *value* is a known database DSN string.

    Parameters
    ----------
    value : str
        Candidate database DSN.

    Returns
    -------
    bool
        ``True`` when *value* uses a recognized database DSN prefix.
    """
    normalized = value.strip().lower()
    return normalized.startswith(DATABASE_SCHEMES) and '+' in normalized


def is_database_url(
    value: str,
) -> bool:
    """
    Return whether *value* is a known database URL string.

    Parameters
    ----------
    value : str
        Candidate database URL.

    Returns
    -------
    bool
        ``True`` when *value* uses a recognized database URL prefix.
    """
    normalized = value.strip().lower()
    return normalized.startswith(DATABASE_SCHEMES) and '://' in normalized
