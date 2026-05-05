"""
:mod:`etlplus.database._sql_dialect` module.

SQL identifier quoting and small dialect-aware helpers.
"""

from __future__ import annotations

from ._enums import DatabaseDialect

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqlDialect',
]


# SECTION: CLASSES ========================================================== #


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

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        dialect: DatabaseDialect | str = DatabaseDialect.SQLITE,
    ) -> None:
        self.dialect = DatabaseDialect.coerce(dialect)

    # -- Instance Methods -- #

    def quote_ident(self, name: str) -> str:
        """
        Return identifier quoted with double quotes.

        Parameters
        ----------
        name : str
            The identifier to quote.
        """
        safe = name.replace('"', '""')

        # We quote everything for simplicity/safety (valid in SQLite/ANSI).
        return f'"{safe}"'

    def quote_table(self, name: str) -> str:
        """
        Quote a possibly-qualified table reference.

        Accepts either "table" or "db.table". For prefixes that are not SQLite
        database aliases (i.e., not "main" or "temp"), the prefix is considered
        a foreign schema name (e.g., "dbo") and is dropped so the reference
        remains valid in SQLite.

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
        n = name.strip()
        if '.' not in n:
            return self.quote_ident(n)

        parts = [p.strip() for p in n.split('.') if p.strip()]
        if not parts:
            raise ValueError(f'Invalid table reference: {name!r}')

        if len(parts) == 1:
            return self.quote_ident(parts[0])

        if self.dialect is not DatabaseDialect.SQLITE:
            return '.'.join(self.quote_ident(part) for part in parts)

        if len(parts) == 2 and parts[0].lower() in ('main', 'temp'):
            return f'{self.quote_ident(parts[0])}.{self.quote_ident(parts[1])}'

        # Unknown prefix (e.g., "dbo"): treat as schema and drop it.
        return self.quote_ident(parts[-1])
