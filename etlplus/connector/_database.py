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
    'sqlserver': 'sqlserver',
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
        explicit = ValueParser.optional_str(obj.get('provider', obj.get('engine')))
        if explicit is not None and (normalized := TextNormalizer.normalize(explicit)):
            return _DATABASE_PROVIDER_ALIASES.get(normalized, normalized)
        if any(obj.get(key) is not None for key in ('project', 'dataset', 'location')):
            return 'bigquery'
        return None

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
            connection_string=ValueParser.optional_str(
                obj.get('connection_string'),
            ),
            provider=cls._provider_from_obj(obj),
            project=ValueParser.optional_str(obj.get('project')),
            dataset=ValueParser.optional_str(obj.get('dataset')),
            location=ValueParser.optional_str(obj.get('location')),
            query=ValueParser.optional_str(obj.get('query')),
            table=ValueParser.optional_str(obj.get('table')),
            mode=ValueParser.optional_str(obj.get('mode')),
        )
