"""
ETLPlus API Client
======================

REST API helpers for client interactions.
"""
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit


# SECTION: CLASSES ========================================================= #


@dataclass(frozen=True, slots=True)
class EndpointClient:
    """
    Immutable registry of API paths rooted at a base URL.

    Parameters
    ----------
    base_url : str
        Absolute base URL, e.g., `"https://api.example.com/v1"`
    endpoints : dict[str, str]
        Mapping of endpoint keys to *relative* paths, e.g.,
        `{"list_users": "/users", "user": "/users/{id}"}`.

    Attributes
    ----------
    base_url : str
        The absolute base URL used as the root for all endpoints (e.g.,
        `"https://api.example.com/v1"`).
    endpoints : dict[str, str]
        Mapping of endpoint keys to *relative* paths, e.g.,
        `{"list_users": "/users", "user": "/users/{id}"}`. A defensive copy of
        the mapping supplied at construction. The dataclass is frozen
        (attributes are read-only), but the dict itself remains mutable. If you
        need deep immutability, store a read-only mapping (e.g., via
        `types.MappingProxyType`).

    Raises
    ------
    ValueError
        If `base_url` is not absolute or if any endpoint key/value is not a
        non-empty `str`.

    Examples
    --------
    >>> ep = Endpoint(
    ...     base_url="https://api.example.com/v1",
    ...     endpoints={"list_users": "users", "user": "/users/{id}"}
    ... )
    >>> ep.url("list_users", {"active": "true"})
    'https://api.example.com/v1/users?active=true'
    """

    # -- Attributes -- #

    base_url: str
    endpoints: dict[str, str]

    # -- Magic Methods (Object Lifecycle) -- #

    def __post_init__(self) -> None:
        # Validate base_url is absolute.
        parts = urlsplit(self.base_url)
        if not parts.scheme or not parts.netloc:
            raise ValueError(
                'base_url must be absolute, e.g. "https://api.example.com"',
            )

        # Defensive copy + validate endpoints.
        eps = dict(self.endpoints)
        for k, v in eps.items():
            if not isinstance(k, str) or not isinstance(v, str) or not v:
                raise ValueError(
                    'endpoints must map str -> non-empty str',
                )
        object.__setattr__(self, 'endpoints', eps)  # Ok w/ frozen dataclasses

    # -- Instance Methods -- #

    def url(
        self,
        endpoint_key: str,
        path_parameters: dict[str, str] | None = None,
        query_parameters: dict[str, str] | None = None,
    ) -> str:
        """
        Build a fully qualified URL for a registered endpoint.

        Parameters
        ----------
        endpoint_key : str
            Key into the `endpoints` mapping whose relative path will be
            resolved against `base_url`.
        path_parameters : dict[str, str], optional
            Values to substitute into placeholders in the endpoint path.
            Placeholders must be written as `{placeholder}` in the relative
            path. Each substituted value is percent-encoded as a single path
            segment (slashes are encoded) to prevent path traversal.
        query_parameters : dict[str, str], optional
            Query parameters to append (and merge with any already present on
            `base_url`). Values are percent-encoded and combined using
            `application/x-www-form-urlencoded` rules.

        Returns
        -------
        str
            The constructed absolute URL.

        Raises
        ------
        KeyError
            If `endpoint_key` is unknown or a required `{placeholder}`
            in the path has no corresponding entry in `path_parameters`.

        Examples
        --------
        >>> ep = Endpoint(
        ...     base_url='https://api.example.com/v1',
        ...     endpoints={
        ...         'user': '/users/{id}',
        ...         'search': '/users'
        ...     }
        ... )
        >>> ep.url('user', path_parameters={'id': '42'})
        'https://api.example.com/v1/users/42'
        >>> ep.url('search', query_parameters={'q': 'Jane Doe', 'page': '2'})
        'https://api.example.com/v1/users?q=Jane+Doe&page=2'
        """
        if endpoint_key not in self.endpoints:
            raise KeyError(f'Unknown endpoint_key: {endpoint_key!r}')

        rel_path = self.endpoints[endpoint_key]

        # Substitute path parameters if provided.
        if '{' in rel_path:
            try:
                encoded = (
                    {
                        k: quote(str(v), safe='')
                        for k, v in path_parameters.items()
                    }
                    if path_parameters
                    else {}
                )
                rel_path = rel_path.format(**encoded)
            except KeyError as e:
                missing = e.args[0]
                raise KeyError(
                    f'Missing path parameter for placeholder: {missing!r}',
                ) from None
            except ValueError as e:
                raise ValueError(
                    f'Invalid path template {rel_path!r}: {e}',
                ) from None

        # Build final absolute URL.
        parts = urlsplit(self.base_url)
        base_path = parts.path.rstrip('/')
        rel_norm = '/' + rel_path.lstrip('/')
        path = (base_path + rel_norm) if base_path else rel_norm

        # Merge base query with provided query_parameters.
        base_q = parse_qsl(parts.query, keep_blank_values=True)
        add_q = list((query_parameters or {}).items())
        qs = urlencode(base_q + add_q, doseq=True)

        return urlunsplit(
            (parts.scheme, parts.netloc, path, qs, parts.fragment),
        )
