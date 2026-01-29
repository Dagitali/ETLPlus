"""
:mod:`etlplus.connector.database` module.

Database connector configuration dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Self
from typing import overload

from ..types import StrAnyMap
from .enums import DataConnectorType
from .utils import _require_name

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .types import ConnectorDbConfigMap
    from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorDb',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorDb:
    """
    Configuration for a database-based data connector.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind, always ``'database'``.
    connection_string : str | None
        Connection string/DSN for the database.
    query : str | None
        Query to execute for extraction (optional).
    table : str | None
        Target/source table name (optional).
    mode : str | None
        Load mode hint (e.g., ``'append'``, ``'replace'``) - future use.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = DataConnectorType.DATABASE
    connection_string: str | None = None
    query: str | None = None
    table: str | None = None
    mode: str | None = None  # append|replace|upsert (future)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorDbConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

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
        name = _require_name(obj, kind='Db')

        return cls(
            name=name,
            type=DataConnectorType.DATABASE,
            connection_string=obj.get('connection_string'),
            query=obj.get('query'),
            table=obj.get('table'),
            mode=obj.get('mode'),
        )
