"""
:mod:`etlplus.api.pagination_client` module.

Client-facing helper that wires pagination configuration, fetch callbacks,
and optional rate limiting into :class:`Paginator` instances.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import cast

from ..types import JSONDict
from ..types import JSONRecords
from .rate_limiter import RateLimiter
from .response import PaginationConfigMap
from .response import PaginationType
from .response import Paginator
from .types import Params
from .types import Url

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PaginationClient',
]


# SECTION: TYPE ALIASES ===================================================== #


type FetchPageFunc = Callable[
    [str, Mapping[str, Any] | None, int | None], Any,
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, kw_only=True)
class PaginationClient:
    """
    Lightweight adapter that runs :class:`Paginator` instances.

    Attributes
    ----------
    pagination : Mapping[str, Any] | None
        Pagination configuration mapping.
    fetch : FetchPageFunc
        Callback used to fetch a single page.
    rate_limiter : RateLimiter | None
        Optional rate limiter invoked between page fetches.
    """

    # -- Attributes -- #

    pagination: Mapping[str, Any] | None
    fetch: FetchPageFunc
    rate_limiter: RateLimiter | None = None

    # -- Internal Attributes -- #

    _ptype: PaginationType | None = field(
        init=False,
        repr=False,
        compare=False,
    )

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        """Normalize and validate pagination configuration."""
        self._ptype = Paginator.detect_type(self.pagination, default=None)

    # -- Properties -- #

    @property
    def is_paginated(self) -> bool:
        """Return ``True`` when a known pagination type is configured."""
        return self._ptype is not None

    # -- Instance Methods -- #

    def collect(
        self,
        url: Url,
        *,
        params: Params | None = None,
    ) -> JSONRecords:
        """
        Collect records across pages into a list.

        Parameters
        ----------
        url : Url
            Base URL to fetch pages from.
        params : Params | None, optional
            Optional query parameters to include in the request.

        Returns
        -------
        JSONRecords
            List of JSON records.
        """
        return list(self.iterate(url, params=params))

    def iterate(
        self,
        url: Url,
        *,
        params: Params | None = None,
    ) -> Generator[JSONDict]:
        """
        Yield records for the configured pagination strategy.

        Parameters
        ----------
        url : Url
            Base URL to fetch pages from.
        params : Params | None, optional
            Optional query parameters to include in the request.
        """
        if not self.is_paginated:
            pg = cast(dict[str, Any], self.pagination or {})
            page_data = self.fetch(url, params, None)
            yield from Paginator.coalesce_records(
                page_data,
                pg.get('records_path'),
                pg.get('fallback_path'),
            )
            return

        paginator = Paginator.from_config(
            cast(PaginationConfigMap, self.pagination),
            fetch=self.fetch,
            rate_limiter=self.rate_limiter,
        )
        yield from paginator.paginate_iter(url, params=params)
