"""
:mod:`etlplus.api.pagination.client` module.

Client-facing pagination driver for REST API responses.

This module wires pagination configuration, fetch callbacks, and optional rate
limiting into :class:`etlplus.api.pagination.Paginator` instances.
"""

from __future__ import annotations

from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import cast

from ...types import JSONDict
from ...types import JSONRecords
from ..rate_limiting import RateLimiter
from ..types import FetchPageCallable
from ..types import RequestOptions
from ..types import Url
from .config import PaginationConfig
from .config import PaginationInput
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
    pagination : PaginationInput
        Pagination configuration mapping or :class:`PaginationConfig`.
    fetch : FetchPageCallable
        Callback used to fetch a single page.
    rate_limiter : RateLimiter | None, optional
        Optional limiter invoked between page fetches.

    Attributes
    ----------
    pagination : PaginationInput
        Resolved pagination configuration.
    fetch : FetchPageCallable
        Stored fetch callback invoked by ``Paginator``.
    rate_limiter : RateLimiter | None
        Limiter applied between requests when configured.
    """

    # -- Attributes -- #

    pagination: PaginationInput
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
        if isinstance(self.pagination, PaginationConfig):
            return self.pagination.type
        return Paginator.detect_type(
            cast(Mapping[str, Any] | None, self.pagination),
            default=None,
        )

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

        Yields
        ------
        Generator[JSONDict]
            Iterator over JSON records from one or more pages.
        """
        effective_request = request or RequestOptions()

        if not self.is_paginated:
            yield from self._iterate_single_page(url, effective_request)
            return

        paginator = Paginator.from_config(
            cast(PaginationInput, self.pagination),
            fetch=self.fetch,
            rate_limiter=self.rate_limiter,
        )
        yield from paginator.paginate_iter(
            url,
            request=effective_request,
        )

    # -- Internal Instance Methods -- #

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
        pg_records_path: str | None
        pg_fallback_path: str | None
        if isinstance(self.pagination, Mapping):
            pg = cast(Mapping[str, Any], self.pagination)
            pg_records_path = cast(str | None, pg.get('records_path'))
            pg_fallback_path = cast(str | None, pg.get('fallback_path'))
        else:
            pg_records_path = getattr(self.pagination, 'records_path', None)
            pg_fallback_path = getattr(self.pagination, 'fallback_path', None)
        page_data = self.fetch(url, request, None)
        yield from Paginator.coalesce_records(
            page_data,
            pg_records_path,
            pg_fallback_path,
        )
