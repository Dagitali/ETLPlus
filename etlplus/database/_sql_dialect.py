"""
:mod:`etlplus.database._sql_dialect` module.

SQL identifier quoting and small dialect-aware helpers.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..utils import TextNormalizer
from ._enums import DatabaseDialect

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqlDialect',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class SqlDialect:
    """
    SQL identifier quoting and small dialect-aware helpers.

    Methods
    -------
    quote_ident : str
        Return identifier quoted with double quotes.

    Notes
    -----
    - Always quotes identifiers to avoid reserved word collisions.
    - Doubles embedded quotes to remain ANSI-compliant.
    - For SQLite, dotted names are interpreted as database.table. This class
        treats unknown prefixes (e.g., "dbo" from SQL Server) as schema-like
        and drops them, yielding just the table component. Known SQLite aliases
        ("main", "temp") are preserved.
    """

    # -- Attributes -- #

    dialect: DatabaseDialect | str = DatabaseDialect.SQLITE

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """Normalize the configured database dialect."""
        self.dialect = DatabaseDialect.coerce(self.dialect)

    # -- Instance Methods -- #

    def quote_ident(
        self,
        name: str,
    ) -> str:
        """
        Return identifier quoted with double quotes.

        Parameters
        ----------
        name : str
            The identifier to quote.

        Returns
        -------
        str
            The quoted identifier.

        Raises
        ------
        ValueError
            If the identifier is empty or blank.
        """
        stripped_name = name.strip()
        if not stripped_name:
            raise ValueError(f'Invalid identifier: {name!r}')

        safe = stripped_name.replace('"', '""')

        # We quote everything for simplicity/safety (valid in SQLite/ANSI).
        return f'"{safe}"'

    def quote_table(
        self,
        name: str,
    ) -> str:
        """
        Quote a possibly-qualified table reference.

        Accepts either "table" or "db.table". For prefixes that are not SQLite
        database aliases (i.e., not "main" or "temp"), the prefix is considered
        a foreign schema name (e.g., "dbo") and is dropped so the reference
        remains valid in SQLite.

        Parameters
        ----------
        name : str
            The table reference to quote.

        Returns
        -------
        str
            The quoted table reference.

        Raises
        ------
        ValueError
            If the table reference is invalid.

        Examples
        --------
        >>> SqlDialect().quote_table('Absences')
        '"Absences"'
        >>> SqlDialect().quote_table('main.Absences')
        '"main"."Absences"'
        >>> SqlDialect().quote_table('dbo.Absences')
        '"Absences"'
        >>> SqlDialect().quote_table('server.db.dbo.Absences')
        '"Absences"'
        """
        stripped_name = name.strip()
        if '.' not in stripped_name:
            return self.quote_ident(stripped_name)

        parts = [part.strip() for part in stripped_name.split('.') if part.strip()]
        if not parts:
            raise ValueError(f'Invalid table reference: {name!r}')

        if len(parts) == 1:
            return self.quote_ident(parts[0])

        if self.dialect is not DatabaseDialect.SQLITE:
            return '.'.join(self.quote_ident(part) for part in parts)

        if len(parts) == 2 and TextNormalizer.normalize(parts[0]) in ('main', 'temp'):
            return f'{self.quote_ident(parts[0])}.{self.quote_ident(parts[1])}'

        # Unknown prefix (e.g., "dbo"): treat as schema and drop it.
        return self.quote_ident(parts[-1])
