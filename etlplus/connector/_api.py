"""
:mod:`etlplus.connector._api` module.

API connector configuration dataclass.

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
from dataclasses import field
from typing import Any
from typing import Self
from typing import TypedDict

from ..api import PaginationConfig
from ..api import PaginationConfigDict
from ..api import RateLimitConfig
from ..api import RateLimitConfigDict
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ..utils._types import StrStrMap
from ._core import ConnectorBase
from ._enums import DataConnectorType
from ._types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorApi',
    'ConnectorApiConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class ConnectorApiConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorApi.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.ConnectorApi.from_obj`
    """

    name: str
    type: ConnectorType
    url: str
    method: str
    headers: StrStrMap
    query_params: StrAnyMap
    pagination: PaginationConfigDict
    rate_limit: RateLimitConfigDict
    api: str
    endpoint: str


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorApi(ConnectorBase):
    """
    Configuration for an API-based data connector.

    Attributes
    ----------
    type : DataConnectorType
        Connector kind, always ``'api'``.
    url : str | None
        Direct absolute URL (when not using ``service``/``endpoint`` refs).
    method : str | None
        Optional HTTP method; typically omitted for sources (defaults to
        GET) and used for targets (e.g., ``'post'``).
    headers : dict[str, str]
        Additional request headers.
    query_params : dict[str, Any]
        Default query parameters.
    pagination : PaginationConfig | None
        Pagination settings (optional).
    rate_limit : RateLimitConfig | None
        Rate limiting settings (optional).
    api : str | None
        Service reference into the pipeline ``apis`` block (a.k.a.
        ``service``).
    endpoint : str | None
        Endpoint name within the referenced service.
    """

    # -- Attributes -- #

    type: DataConnectorType = DataConnectorType.API

    # Direct form
    url: str | None = None
    # Optional HTTP method; typically omitted for sources (defaults to GET)
    # at runtime) and used for targets (e.g., 'post', 'put').
    method: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, Any] = field(default_factory=dict)
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # Reference form (to top-level APIs/endpoints)
    api: str | None = None
    endpoint: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorApi`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.
        """
        headers = MappingParser.to_str_dict(
            MappingParser.optional(obj.get('headers')),
        )

        return cls(
            name=cls._name_from_obj(obj),
            url=ValueParser.optional_str(obj.get('url')),
            method=ValueParser.optional_str(obj.get('method')),
            headers=headers,
            query_params=MappingParser.to_dict(obj.get('query_params')),
            pagination=PaginationConfig.from_obj(obj.get('pagination')),
            rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            api=ValueParser.optional_str(obj.get('api') or obj.get('service')),
            endpoint=ValueParser.optional_str(obj.get('endpoint')),
        )
