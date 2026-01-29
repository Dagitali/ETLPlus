"""
:mod:`etlplus.connector.api` module.

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
from typing import overload

from ..api import PaginationConfig
from ..api import PaginationConfigMap
from ..api import RateLimitConfig
from ..api import RateLimitConfigMap
from ..types import StrAnyMap
from ..types import StrStrMap
from ..utils import cast_str_dict
from ..utils import coerce_dict
from ..utils import maybe_mapping
from .core import ConnectorBase
from .enums import DataConnectorType
from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorApi',
    'ConnectorApiConfigMap',
]


# SECTION: TYPED DICTS ====================================================== #


class ConnectorApiConfigMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorApi.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.api.ConnectorApi.from_obj`
    """

    name: str
    type: ConnectorType
    url: str
    method: str
    headers: StrStrMap
    query_params: StrAnyMap
    pagination: PaginationConfigMap
    rate_limit: RateLimitConfigMap
    api: str
    endpoint: str


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorApi(ConnectorBase):
    """
    Configuration for an API-based data connector.

    Attributes
    ----------
    type : ConnectorType
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

    type: ConnectorType = DataConnectorType.API

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
    @overload
    def from_obj(cls, obj: ConnectorApiConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

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
        name = cls._require_name(obj, kind='Api')
        headers = cast_str_dict(maybe_mapping(obj.get('headers')))

        return cls(
            name=name,
            url=obj.get('url'),
            method=obj.get('method'),
            headers=headers,
            query_params=coerce_dict(obj.get('query_params')),
            pagination=PaginationConfig.from_obj(obj.get('pagination')),
            rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            api=obj.get('api') or obj.get('service'),
            endpoint=obj.get('endpoint'),
        )
