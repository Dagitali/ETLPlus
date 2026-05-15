"""
:mod:`etlplus.connector._database` module.

Database connector configuration dataclass.

Notes
-----
- TypedDicts in this module are intentionally ``total=False`` and are not
    enforced at runtime.
- :meth:`*.from_obj` constructors accept :class:`Mapping[str, Any]` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final
from typing import Self
from typing import TypedDict

from ..utils import TextNormalizer
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._core import ConnectorBase
from ._enums import DataConnectorType
from ._types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorDb',
    'ConnectorDbConfigDict',
]


_DATABASE_PROVIDER_ALIASES: Final[dict[str, str]] = {
    'bigquery': 'bigquery',
    'bq': 'bigquery',
    'gcp-bigquery': 'bigquery',
    'google-bigquery': 'bigquery',
    'mssql': 'sqlserver',
    'postgresql': 'postgres',
    'sf': 'snowflake',
    'snowflake-db': 'snowflake',
    'snowflake': 'snowflake',
    'sqlserver': 'sqlserver',
}

_DATABASE_PROVIDER_DISPLAY_NAMES: Final[dict[str, str]] = {
    'bigquery': 'BigQuery',
    'snowflake': 'Snowflake',
}

_DATABASE_PROVIDER_HINT_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    'bigquery': ('project', 'dataset', 'location'),
    'snowflake': ('account', 'database', 'schema', 'warehouse'),
}

_DATABASE_PROVIDER_REQUIRED_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    'bigquery': ('project', 'dataset'),
    'snowflake': ('account', 'database', 'schema'),
}

_DATABASE_PROVIDER_MISSING_CONNECTION_ISSUES: Final[dict[str, str]] = {
    'bigquery': 'missing connection_string or bigquery project/dataset',
    'snowflake': 'missing connection_string or snowflake account/database/schema',
}


# SECTION: TYPED DICTS ====================================================== #


class ConnectorDbConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorDb.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.ConnectorDb.from_obj`
    """

    name: str
    type: ConnectorType
    connection_string: str
    provider: str
    project: str
    dataset: str
    location: str
    account: str
    database: str
    schema: str
    warehouse: str
    query: str
    table: str
    mode: str


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorDb(ConnectorBase):
    """
    Configuration for a database-based data connector.

    Attributes
    ----------
    type : DataConnectorType
        Connector kind, always ``'database'``.
    connection_string : str | None
        Connection string/DSN for the database.
    provider : str | None
        Optional normalized database provider hint.
    project : str | None
        Optional provider-specific project identifier.
    dataset : str | None
        Optional provider-specific dataset identifier.
    location : str | None
        Optional provider-specific regional location hint.
    account : str | None
        Optional Snowflake account identifier.
    database : str | None
        Optional Snowflake database identifier.
    schema : str | None
        Optional Snowflake schema identifier.
    warehouse : str | None
        Optional Snowflake warehouse identifier.
    query : str | None
        Query to execute for extraction (optional).
    table : str | None
        Target/source table name (optional).
    mode : str | None
        Load mode hint: ``'append'``, ``'replace'``, ``'upsert'`` (future use).
    """

    # -- Attributes -- #

    type: DataConnectorType = DataConnectorType.DATABASE
    connection_string: str | None = None
    provider: str | None = None
    project: str | None = None
    dataset: str | None = None
    location: str | None = None
    account: str | None = None
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    query: str | None = None
    table: str | None = None
    mode: str | None = None

    # -- Internal Class Methods -- #

    @classmethod
    def _provider_from_obj(
        cls,
        obj: StrAnyMap,
    ) -> str | None:
        """Return one normalized provider hint when present."""
        explicit = cls._optional_str(obj, 'provider', 'engine')
        if explicit is not None and (normalized := TextNormalizer.normalize(explicit)):
            return cls.normalize_provider(normalized)
        for provider, fields in _DATABASE_PROVIDER_HINT_FIELDS.items():
            if any(obj.get(key) is not None for key in fields):
                return provider
        return None

    # -- Class Methods -- #

    @classmethod
    def missing_provider_fields(
        cls,
        obj: Mapping[str, object] | object,
        *,
        provider: str | None = None,
    ) -> tuple[str, ...]:
        """Return required provider fields missing from *obj*."""
        normalized = cls.normalize_provider(
            provider
            or (
                ValueParser.optional_str(obj.get('provider'))
                if isinstance(obj, Mapping)
                else ValueParser.optional_str(getattr(obj, 'provider', None))
            ),
        )
        if normalized is None:
            return ()

        missing: list[str] = []
        for field in cls.provider_required_fields(normalized):
            value = (
                obj.get(field)
                if isinstance(obj, Mapping)
                else getattr(obj, field, None)
            )
            if ValueParser.optional_str(value) is None:
                missing.append(field)
        return tuple(missing)

    @classmethod
    def normalize_provider(
        cls,
        provider: str | None,
    ) -> str | None:
        """Return one normalized provider name when present."""
        if provider is None:
            return None
        normalized = TextNormalizer.normalize(provider)
        if not normalized:
            return None
        return _DATABASE_PROVIDER_ALIASES.get(normalized, normalized)

    @classmethod
    def provider_display_name(
        cls,
        provider: str | None,
    ) -> str | None:
        """Return one human-readable provider name when supported."""
        normalized = cls.normalize_provider(provider)
        if normalized is None:
            return None
        return _DATABASE_PROVIDER_DISPLAY_NAMES.get(normalized, normalized.title())

    @classmethod
    def provider_required_fields(
        cls,
        provider: str | None,
    ) -> tuple[str, ...]:
        """Return required provider-specific metadata fields."""
        normalized = cls.normalize_provider(provider)
        if normalized is None:
            return ()
        return _DATABASE_PROVIDER_REQUIRED_FIELDS.get(normalized, ())

    @classmethod
    def provider_missing_connection_guidance(
        cls,
        provider: str | None,
    ) -> str | None:
        """Return one normalized remediation string for provider metadata gaps."""
        normalized = cls.normalize_provider(provider)
        if normalized is None:
            return None
        fields = cls.provider_required_fields(normalized)
        if not fields:
            return None

        if len(fields) == 1:
            field_text = f'"{fields[0]}"'
        elif len(fields) == 2:
            field_text = f'both "{fields[0]}" and "{fields[1]}"'
        else:
            field_text = ', '.join(f'"{field}"' for field in fields[:-1])
            field_text += f', and "{fields[-1]}"'

        display_name = cls.provider_display_name(normalized) or normalized.title()
        return (
            'Set "connection_string" to a database DSN or SQLAlchemy-style '
            f'URL, or define {field_text} for this {display_name} connector.'
        )

    @classmethod
    def provider_missing_connection_issue(
        cls,
        provider: str | None,
    ) -> str | None:
        """Return the normalized missing-metadata issue id for *provider*."""
        normalized = cls.normalize_provider(provider)
        if normalized is None:
            return None
        return _DATABASE_PROVIDER_MISSING_CONNECTION_ISSUES.get(normalized)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorDb`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.
        """
        return cls(
            name=cls._name_from_obj(obj),
            provider=cls._provider_from_obj(obj),
            connection_string=cls._optional_str(obj, 'connection_string'),
            project=cls._optional_str(obj, 'project'),
            dataset=cls._optional_str(obj, 'dataset'),
            location=cls._optional_str(obj, 'location'),
            account=cls._optional_str(obj, 'account'),
            database=cls._optional_str(obj, 'database'),
            schema=cls._optional_str(obj, 'schema'),
            warehouse=cls._optional_str(obj, 'warehouse'),
            query=cls._optional_str(obj, 'query'),
            table=cls._optional_str(obj, 'table'),
            mode=cls._optional_str(obj, 'mode'),
        )
