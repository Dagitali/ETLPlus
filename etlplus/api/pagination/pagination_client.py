"""
:mod:`etlplus.api.pagination_client` module.

Client-facing helper that wires pagination configuration, fetch callbacks,
and optional rate limiting into :class:`Paginator` instances.
"""
from __future__ import annotations

from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import cast

from ...types import JSONDict
from ...types import JSONRecords
from ..rate_limiter import RateLimiter
from ..types import FetchPageCallable
from ..types import RequestOptions
from ..types import Url
from .config import PaginationConfigMap
from .config import PaginationType
from .paginator import Paginator

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'PaginationClient',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, kw_only=True)
class PaginationClient:
    """
    Drive :class:`Paginator` instances with shared guardrails.

    Parameters
    ----------
    pagination : Mapping[str, Any] | None
        Pagination configuration mapping.
    fetch : FetchPageCallable
        Callback used to fetch a single page.
    rate_limiter : RateLimiter | None, optional
        Optional limiter invoked between page fetches.

    Attributes
    ----------
    pagination : Mapping[str, Any] | None
        Resolved pagination configuration.
    fetch : FetchPageCallable
        Stored fetch callback invoked by ``Paginator``.
    rate_limiter : RateLimiter | None
        Limiter applied between requests when configured.
    """

    # -- Attributes -- #

    pagination: Mapping[str, Any] | None
    fetch: FetchPageCallable
    rate_limiter: RateLimiter | None = None

    # -- Properties -- #

    @property
    def is_paginated(self) -> bool:
        """Return ``True`` when a known pagination type is configured."""
        return self.pagination_type is not None

    @property
    def pagination_type(self) -> PaginationType | None:
        """Return the normalized pagination type when available."""
        return Paginator.detect_type(self.pagination, default=None)

    # -- Instance Methods -- #

    def collect(
        self,
        url: Url,
        *,
        request: RequestOptions | None = None,
    ) -> JSONRecords:
        """
        Collect records across pages into a list.

        Parameters
        ----------
        url : Url
            Base URL to fetch pages from.
        request : RequestOptions | None, optional
            Snapshot of request metadata (params/headers/timeout) to clone
            for this invocation.

        Returns
        -------
        JSONRecords
            List of JSON records.
        """
        return list(self.iterate(url, request=request))

    def iterate(
        self,
        url: Url,
        *,
        request: RequestOptions | None = None,
    ) -> Generator[JSONDict]:
        """
        Yield records for the configured pagination strategy.

        Parameters
        ----------
        url : Url
            Base URL to fetch pages from.
        request : RequestOptions | None, optional
            Snapshot of request metadata (params/headers/timeout) to clone
            for this invocation.
        """
        effective_request = request or RequestOptions()

        if not self.is_paginated:
            yield from self._iterate_single_page(url, effective_request)
            return

        paginator = Paginator.from_config(
            cast(PaginationConfigMap, self.pagination),
            fetch=self.fetch,
            rate_limiter=self.rate_limiter,
        )
        yield from paginator.paginate_iter(
            url,
            request=effective_request,
        )

    # -- InternalInstance Methods -- #

    def _iterate_single_page(
        self,
        url: Url,
        request: RequestOptions,
    ) -> Generator[JSONDict]:
        """
        Iterate records for non-paginated responses.

        Parameters
        ----------
        url : Url
            Base URL to fetch pages from.
        request : RequestOptions
            Request metadata to forward to the fetch callback.

        Yields
        ------
        Generator[JSONDict]
            JSON records from the response.
        """
        pg = cast(dict[str, Any], self.pagination or {})
        page_data = self.fetch(url, request, None)
        yield from Paginator.coalesce_records(
            page_data,
            pg.get('records_path'),
            pg.get('fallback_path'),
        )
