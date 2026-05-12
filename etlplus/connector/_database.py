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
from typing import Self
from typing import TypedDict

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
    query : str | None
        Query to execute for extraction (optional).
    table : str | None
        Target/source table name (optional).
    mode : str | None
        Load mode hint (e.g., ``'append'``, ``'replace'``) - future use.
    """

    # -- Attributes -- #

    type: DataConnectorType = DataConnectorType.DATABASE
    connection_string: str | None = None
    query: str | None = None
    table: str | None = None
    mode: str | None = None  # append|replace|upsert (future)

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
            query=ValueParser.optional_str(obj.get('query')),
            table=ValueParser.optional_str(obj.get('table')),
            mode=ValueParser.optional_str(obj.get('mode')),
        )
