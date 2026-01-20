"""
:mod:`etlplus.api.enums` module.

File-specific REST API-aligned enums and helpers.
"""

from __future__ import annotations

from ..enums import CoercibleStrEnum

# SECTION: EXPORTS ========================================================= #


__all__ = [
    # Enums
    'HttpMethod',
]


# SECTION: ENUMS ============================================================ #


class HttpMethod(CoercibleStrEnum):
    """Supported HTTP verbs that accept JSON payloads."""

    # -- Constants -- #

    CONNECT = 'connect'
    DELETE = 'delete'
    GET = 'get'
    HEAD = 'head'
    OPTIONS = 'options'
    PATCH = 'patch'
    POST = 'post'
    PUT = 'put'
    TRACE = 'trace'

    # -- Getters -- #

    @property
    def allows_body(self) -> bool:
        """
        Whether the method typically allows a request body.

        Notes
        -----
        - RFCs do not strictly forbid bodies on some other methods (e.g.,
            ``DELETE``), but many servers/clients do not expect them. We mark
            ``POST``, ``PUT``, and ``PATCH`` as True.
        """
        return self in {HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH}
