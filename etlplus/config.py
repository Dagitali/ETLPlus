"""
etl.config
==========

A module defining pipeline configuration models.

These classes represent a tolerant schema for pipeline YAML files like
`in/pipeline.yml`. They aim to cover common shapes while allowing
provider-specific options to pass through as dictionaries.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from .file import read_yaml
from .types import StrAnyMap
from .types import StrPath
from .types import StrStrMap


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _deep_substitute(
    value: Any,
    vars_map: StrAnyMap,
    env_map: StrStrMap,
) -> Any:
    """
    Recursively substitute ${VAR} tokens using vars and environment.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : StrAnyMap
        Mapping of variable names to their replacement values.
    env_map : StrStrMap
        Mapping of environment variable names to their replacement values.

    Returns
    -------
    Any
        The value with substitutions applied.
    """

    if isinstance(value, str):
        out = value
        for name, replacement in vars_map.items():
            out = out.replace(f'${{{name}}}', str(replacement))
        for name, replacement in env_map.items():
            out = out.replace(f'${{{name}}}', str(replacement))
        return out
    if isinstance(value, dict):
        return {
            k: _deep_substitute(v, vars_map, env_map)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_deep_substitute(v, vars_map, env_map) for v in value]

    return value


# SECTION: FUNCTIONS ======================================================== #


def load_pipeline_config(
    path: StrPath,
    *,
    substitute: bool = False,
    env: StrStrMap | None = None,
) -> PipelineConfig:
    """
    Read a pipeline YAML file into a PipelineConfig dataclass.

    Delegates to PipelineConfig.from_yaml for the actual construction and
    optional variable substitution.

    Parameters
    ----------
    path : StrPath
        Path to the pipeline YAML file.
    substitute : bool, optional
        Whether to perform variable substitution, by default False.
    env : StrStrMap | None, optional
        Environment variable mapping to use for substitution. If None,
        os.environ will be used, by default None.
    """

    return PipelineConfig.from_yaml(path, substitute=substitute, env=env)


# SECTION: CLASSES (Pagination/Rate) ======================================== #


@dataclass(slots=True)
class PaginationConfig:
    """
    Configuration for pagination in API requests.

    Attributes
    ----------
    type : str | None
        Pagination type: "page", "offset", or "cursor".
    page_param : str | None
        Name of the page parameter.
    size_param : str | None
        Name of the page size parameter.
    start_page : int | None
        Starting page number.
    page_size : int | None
        Number of records per page.
    cursor_param : str | None
        Name of the cursor parameter.
    cursor_path : str | None
        JSONPath expression to extract the cursor from the response.
    start_cursor : str | int | None
        Starting cursor value.
    records_path : str | None
        JSONPath expression to extract the records from the response.
    max_pages : int | None
        Maximum number of pages to retrieve.
    max_records : int | None
        Maximum number of records to retrieve.

    Methods
    -------
    from_obj(obj: Any) -> PaginationConfig | None
        Create a PaginationConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    type: str | None = None  # "page" | "offset" | "cursor"

    # Page/offset
    page_param: str | None = None
    size_param: str | None = None
    start_page: int | None = None
    page_size: int | None = None

    # Cursor
    cursor_param: str | None = None
    cursor_path: str | None = None
    start_cursor: str | int | None = None

    # General
    records_path: str | None = None
    max_pages: int | None = None
    max_records: int | None = None

    # -- Static Methods -- #

    @staticmethod
    def from_obj(obj: Any) -> PaginationConfig | None:
        if not isinstance(obj, dict):
            return None
        return PaginationConfig(
            type=str(obj.get('type')) if obj.get('type') is not None else None,
            page_param=obj.get('page_param'),
            size_param=obj.get('size_param'),
            start_page=obj.get('start_page'),
            page_size=obj.get('page_size'),
            cursor_param=obj.get('cursor_param'),
            cursor_path=obj.get('cursor_path'),
            start_cursor=obj.get('start_cursor'),
            records_path=obj.get('records_path'),
            max_pages=obj.get('max_pages'),
            max_records=obj.get('max_records'),
        )


@dataclass(slots=True)
class RateLimitConfig:
    """
    Configuration for rate limiting in API requests.

    Attributes
    ----------
    sleep_seconds : float | None
        Number of seconds to sleep between requests.
    max_per_sec : float | None
        Maximum number of requests per second.

    Methods
    -------
    from_obj(obj: Any) -> RateLimitConfig | None
        Create a RateLimitConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    sleep_seconds: float | None = None
    max_per_sec: float | None = None

    # -- Static Methods -- #

    @staticmethod
    def from_obj(obj: Any) -> RateLimitConfig | None:
        if not isinstance(obj, dict):
            return None
        return RateLimitConfig(
            sleep_seconds=obj.get('sleep_seconds'),
            max_per_sec=obj.get('max_per_sec'),
        )


# SECTION: CLASSES (REST APIS) ============================================== #


@dataclass(slots=True)
class ApiProfileConfig:
    """
    Profile configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Base URL for the API.
    headers : dict[str, str]
        Default headers for the API.
    base_path : str | None
        Optional base path to prepend to endpoint paths.
    auth : dict[str, Any]
        Optional auth block (provider-specific shape, passed through).
    """

    # -- Attributes -- #

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    base_path: str | None = None
    auth: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ApiConfig:
    """
    Configuration for a REST API service.

    Attributes
    ----------
    base_url : str
        Base URL for the API (may be derived from a selected profile). This
        field is always required and must be a non-empty string.
    headers : dict[str, str]
        Default headers for the API (may be derived from a selected profile).
    endpoints : dict[str, EndpointConfig]
        Configured endpoints for the API.
    profiles : dict[str, ApiProfileConfig]
        Optional named profiles providing per-environment base_url/headers.

    Methods
    -------
    from_obj(obj: Any) -> ApiConfig
        Create an ApiConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    base_url: str
    headers: dict[str, str] = field(default_factory=dict)
    endpoints: dict[str, EndpointConfig] = field(default_factory=dict)
    profiles: dict[str, ApiProfileConfig] = field(default_factory=dict)

    # -- Instance Methods -- #

    def build_endpoint_url(
        self, endpoint: EndpointConfig,
    ) -> str:
        """
        Compose a full URL from base_url, base_path, and endpoint.path.

        This mirrors EndpointClient.url path-join semantics, but uses
        the model's effective_base_url() which includes profile base_path.

        Parameters
        ----------
        endpoint : EndpointConfig
            The endpoint configuration to build the URL for.

        Returns
        -------
        str
            The composed URL for the endpoint.
        """

        base = self.effective_base_url()
        parts = urlsplit(base)
        base_path = parts.path.rstrip('/')
        rel_norm = '/' + endpoint.path.lstrip('/')
        path = (base_path + rel_norm) if base_path else rel_norm

        return urlunsplit(
            (parts.scheme, parts.netloc, path, parts.query, parts.fragment),
        )

    def effective_base_path(self) -> str | None:
        """
        Return the selected profile's base_path, if any.

        Selection mirrors base_url/header selection: use the 'default'
        profile when present, otherwise the first available profile.
        For legacy flat shapes (no profiles), returns None.

        Returns
        -------
        str | None
            The effective base path for the API.
        """

        if not self.profiles:
            return None
        prof_name = (
            'default' if 'default' in self.profiles
            else next(iter(self.profiles.keys()))
        )

        return getattr(self.profiles[prof_name], 'base_path', None)

    def effective_base_url(self) -> str:
        """
        Compute base_url combined with effective base_path, if present.

        This composes the URL path as:
          urlsplit(base_url).path + '/' + base_path + ...
        ensuring single slashes between segments.

        Returns
        -------
        str
            The composed URL for the endpoint.
        """

        parts = urlsplit(self.base_url)
        base_path = parts.path.rstrip('/')
        extra = self.effective_base_path()
        extra_norm = ('/' + extra.lstrip('/')) if extra else ''
        path = (base_path + extra_norm) if (base_path or extra_norm) else ''

        return urlunsplit(
            (parts.scheme, parts.netloc, path, parts.query, parts.fragment),
        )

    # -- Static Methods -- #

    @staticmethod
    def from_obj(obj: Any) -> ApiConfig:
        """
        Create an ApiConfig instance from a dictionary-like object.

        Parameters
        ----------
        obj : Any
            The object to create the ApiConfig from.

        Returns
        -------
        ApiConfig
            The created ApiConfig instance.

        Raises
        ------
        TypeError
            If the input object is not a valid mapping.
        TypeError
            If the input object is missing required fields.
        """

        if not isinstance(obj, dict):
            raise TypeError('ApiConfig must be a mapping')

        # Optional: profiles structure
        profiles_raw = obj.get('profiles', {}) or {}
        profiles: dict[str, ApiProfileConfig] = {}
        if isinstance(profiles_raw, dict):
            for name, p in profiles_raw.items():
                if isinstance(p, dict):
                    p_base = p.get('base_url')
                    if not isinstance(p_base, str):
                        raise TypeError(
                            'ApiProfileConfig requires "base_url" (str)',
                        )

                    # Merge defaults.headers (low precedence) with headers.
                    dflt_headers_raw = (
                        (p.get('defaults', {}) or {}).get('headers', {})
                    )
                    dflt_headers = {
                        k: str(v) for k, v in (dflt_headers_raw or {}).items()
                    }
                    p_headers = {
                        k: str(v)
                        for k, v in (p.get('headers', {}) or {}).items()
                    }
                    merged_headers = {**dflt_headers, **p_headers}
                    base_path = p.get('base_path')
                    auth = dict(p.get('auth', {}) or {})
                    profiles[str(name)] = ApiProfileConfig(
                        base_url=p_base,
                        headers=merged_headers,
                        base_path=base_path,
                        auth=auth,
                    )

        # Top-level fallbacks (or legacy flat shape)
        tl_base = obj.get('base_url')
        tl_headers = {
            k: str(v)
            for k, v in (obj.get('headers', {}) or {}).items()
        }

        # Determine effective base_url/headers for backward compatibility
        # Always compute a concrete str for base_url.
        base_url: str
        headers: dict[str, str] = {}
        if profiles:
            # Choose a default profile: explicit 'default' else first key
            prof_name = 'default' if 'default' in profiles else (
                next(iter(profiles.keys()))
            )
            base_url = profiles[prof_name].base_url
            headers = dict(profiles[prof_name].headers)
            # Merge in top-level headers as overrides if provided
            if tl_headers:
                headers |= tl_headers
        else:
            # Legacy flat shape must provide base_url
            if not isinstance(tl_base, str):
                raise TypeError('ApiConfig requires "base_url" (str)')
            base_url = tl_base
            headers = tl_headers
        raw_eps = obj.get('endpoints', {}) or {}
        eps: dict[str, EndpointConfig] = {}
        if isinstance(raw_eps, dict):
            for name, ep in raw_eps.items():
                eps[str(name)] = EndpointConfig.from_obj(ep)

        return ApiConfig(
            base_url=base_url,
            headers=headers,
            endpoints=eps,
            profiles=profiles,
        )


@dataclass(slots=True)
class EndpointConfig:
    """
    Configuration for a single API endpoint.

    Attributes
    ----------
    path : str
        Endpoint path (relative to base URL).
    params : dict[str, Any]
        Default query parameters for the endpoint.
    pagination : PaginationConfig | None
        Pagination configuration for the endpoint.
    rate_limit : RateLimitConfig | None
        Rate limiting configuration for the endpoint.

    Methods
    -------
    from_obj(obj: Any) -> EndpointConfig
        Create an EndpointConfig instance from a dictionary-like object.
    """

    # -- Attributes -- #

    path: str
    method: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    path_params: dict[str, Any] = field(default_factory=dict)
    body: Any | None = None
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # -- Static Methods -- #

    @staticmethod
    def from_obj(obj: Any) -> EndpointConfig:
        # Allow either a bare string path or a mapping with explicit fields
        if isinstance(obj, str):
            return EndpointConfig(path=obj)
        if isinstance(obj, dict):
            path = obj.get('path')

            # Tolerate configs that provide the path directly at key level
            if path is None and 'url' in obj:
                path = obj.get('url')
            if not isinstance(path, str):
                raise TypeError('EndpointConfig requires a "path" (str)')

            # Accept only explicit query_params for URL query string pairs.
            # This removes ambiguity compared to a generic "params" key.
            query_params = dict(obj.get('query_params', {}) or {})

            return EndpointConfig(
                path=path,
                method=obj.get('method', 'GET'),
                params=query_params,
                path_params=dict(obj.get('path_params', {}) or {}),
                body=obj.get('body'),
                pagination=PaginationConfig.from_obj(obj.get('pagination')),
                rate_limit=RateLimitConfig.from_obj(obj.get('rate_limit')),
            )

        raise TypeError('Invalid endpoint config: must be str or mapping')


# SECTION: CLASSES (SOURCES) ================================================ #


@dataclass(slots=True)
class SourceApi:
    """
    Configuration for an API-based data source.

    Attributes
    ----------
    name : str
        Name of the source.
    type : str
        Type of the source (default: "api").
    url : str | None
        URL of the API endpoint.
    params : dict[str, Any]
        Query parameters for the API request.
    headers : dict[str, str]
        Headers to include in the API request.
    pagination : PaginationConfig | None
        Pagination configuration for the API request.
    rate_limit : RateLimitConfig | None
        Rate limit configuration for the API request.
    api : str | None
        Reference to a top-level API configuration.
    endpoint : str | None
        Reference to a specific API endpoint.

    Methods
    -------
    from_obj(obj: Any) -> SourceApi
        Create a SourceApi instance from a dictionary-like object.
    """

    # -- Attributes -- #

    name: str
    type: str = 'api'

    # Direct form
    url: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None

    # Reference form (to top-level apis/endpoints)
    api: str | None = None
    endpoint: str | None = None


@dataclass(slots=True)
class SourceDb:
    """
    Configuration for a database-based data source.

    Attributes
    ----------
    name : str
        Name of the source.
    type : str
        Type of the source (default: "database").
    connection_string : str | None
        Connection string for the database.
    """

    # -- Attributes -- #

    name: str
    type: str = 'database'
    connection_string: str | None = None
    query: str | None = None


@dataclass(slots=True)
class SourceFile:
    """
    Configuration for a file-based data source.

    Attributes
    ----------
    name : str
        Name of the source.
    type : str
        Type of the source (default: "file").
    format : str | None
        Format of the file (e.g., "csv", "json").
    path : str | None
        Path to the file.
    options : dict[str, Any]
        Additional options for the file source.

    Methods
    -------
    from_obj(obj: Any) -> SourceFile
        Create a SourceFile instance from a dictionary-like object.
    """

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


# SECTION: CLASSES (TARGETS) ================================================ #


@dataclass(slots=True)
class TargetApi:
    """
    Configuration for an API-based data target.

    Attributes
    ----------
    name : str
        Name of the target.
    type : str
        Type of the target (default: "api").
    url : str | None
        URL of the API endpoint.
    method : str | None
        HTTP method to use (e.g., "POST", "PUT").
    headers : dict[str, str]
        Headers to include in the API request.
    """

    # -- Attributes -- #

    name: str
    type: str = 'api'
    url: str | None = None
    method: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class TargetDb:
    """
    Configuration for a database-based data target.

    Attributes
    ----------
    name : str
        Name of the target.
    type : str
        Type of the target (default: "database").
    connection_string : str | None
        Connection string for the database.
    table : str | None
        Target table name.
    mode : str | None
        Load mode (e.g., "append", "replace", "upsert").
    """

    # -- Attributes -- #

    name: str
    type: str = 'database'
    connection_string: str | None = None
    table: str | None = None
    mode: str | None = None  # append|replace|upsert (future)


@dataclass(slots=True)
class TargetFile:
    """
    Configuration for a file-based data target.

    Attributes
    ----------
    name : str
        Name of the target.
    type : str
        Type of the target (default: "file").
    format : str | None
        Format of the file (e.g., "csv", "json").
    path : str | None
        Path to the file.

    Methods
    -------
    from_obj(obj: Any) -> TargetFile
        Create a TargetFile instance from a dictionary-like object.
    """

    # -- Attributes -- #

    name: str
    type: str = 'file'
    format: str | None = None
    path: str | None = None


# SECTION: CLASSES (VALIDATIONS, TRANSFORMATIONS, JOBS) ===================== #


@dataclass(slots=True)
class ExtractRef:
    """
    Reference to a data source for extraction.

    Attributes
    ----------
    source : str
        Name of the data source.
    options : dict[str, Any]
        Additional options for the extraction.
    """

    # -- Attributes -- #

    source: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JobConfig:
    """
    Configuration for a data processing job.

    Attributes
    ----------
    name : str
        Name of the job.
    description : str | None
        Description of the job.
    extract : ExtractRef | None
        Reference to the extraction configuration.
    """

    # -- Attributes -- #

    name: str
    description: str | None = None
    extract: ExtractRef | None = None
    validate: ValidationRef | None = None
    transform: TransformRef | None = None
    load: LoadRef | None = None


@dataclass(slots=True)
class LoadRef:
    """
    Reference to a data target for loading.

    Attributes
    ----------
    target : str
        Name of the data target.
    overrides : dict[str, Any]
        Additional options for the loading process.
    """

    # -- Attributes -- #

    target: str
    overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TransformRef:
    """
    Reference to a transformation pipeline.

    Attributes
    ----------
    pipeline : str
        Name of the transformation pipeline.
    """

    # -- Attributes -- #

    pipeline: str


@dataclass(slots=True)
class ValidationRef:
    """
    Reference to a validation rule set.

    Attributes
    ----------
    ruleset : str
        Name of the validation ruleset.
    severity : str | None
        Severity level ('warn' or 'error').
    phase : str | None
        Phase to apply the validation ('before_transform', 'after_transform',
        or 'both').
    """

    # -- Attributes -- #

    ruleset: str
    severity: str | None = None  # warn|error
    phase: str | None = None     # before_transform|after_transform|both


# SECTION: CLASSES (PROFILE) ================================================ #


@dataclass(slots=True)
class ProfileConfig:
    """
    Configuration for pipeline profiles.

    Attributes
    ----------
    default_target : str | None
        Default target name for the profile.
    env : dict[str, str]
        Environment variables for the profile.
    """

    # -- Attributes -- #

    default_target: str | None = None
    env: dict[str, str] = field(default_factory=dict)


# SECTION: CLASSES (TOP-LEVEL) ============================================== #


@dataclass(slots=True)
class PipelineConfig:
    """
    Configuration for the data processing pipeline.

    Attributes
    ----------
    name : str | None
        Name of the pipeline.
    version : str | None
        Version of the pipeline.
    profile : ProfileConfig
        Profile configuration.
    vars : dict[str, Any]
        Variables for substitution.
    apis : dict[str, ApiConfig]
        Configured APIs.
    databases : dict[str, dict[str, Any]]
        Configured databases.
    file_systems : dict[str, dict[str, Any]]
        Configured file systems.
    sources : list[Source]
        Data sources.
    validations : dict[str, dict[str, Any]]
        Validation rule sets.
    transforms : dict[str, dict[str, Any]]
        Transformation pipelines.
    targets : list[Target]
        Data targets.
    jobs : list[JobConfig]
        Data processing jobs.
    """

    # -- Attributes -- #

    name: str | None = None
    version: str | None = None
    profile: ProfileConfig = field(default_factory=ProfileConfig)
    vars: dict[str, Any] = field(default_factory=dict)

    apis: dict[str, ApiConfig] = field(default_factory=dict)
    databases: dict[str, dict[str, Any]] = field(default_factory=dict)
    file_systems: dict[str, dict[str, Any]] = field(default_factory=dict)

    sources: list[Source] = field(default_factory=list)
    validations: dict[str, dict[str, Any]] = field(default_factory=dict)
    transforms: dict[str, dict[str, Any]] = field(default_factory=dict)
    targets: list[Target] = field(default_factory=list)
    jobs: list[JobConfig] = field(default_factory=list)

    # -- Class Methods -- #

    @classmethod
    def from_yaml(
        cls,
        path: StrPath,
        *,
        substitute: bool = False,
        env: StrStrMap | None = None,
    ) -> PipelineConfig:
        """
        Create a PipelineConfig instance from a YAML file.

        This convenience constructor mirrors load_pipeline_config, reading the
        YAML, validating its root type, constructing the config via from_dict,
        and optionally performing ${VAR} substitutions using cfg.vars combined
        with the provided env mapping (or os.environ if omitted).

        Parameters
        ----------
        path : StrPath
            Path to the pipeline YAML file.
        substitute : bool, optional
            Whether to perform variable substitution, by default False.
        env : StrStrMap | None, optional
            Environment variable mapping to use for substitution. If None,
            os.environ will be used, by default None.

        Returns
        -------
        PipelineConfig
            The created PipelineConfig instance.
        """

        raw = read_yaml(Path(path))
        if not isinstance(raw, dict):
            raise TypeError('Pipeline YAML must have a mapping/object root')

        cfg = cls.from_dict(raw)

        if substitute:
            # Merge order: profile.env first (lowest), then provided env or
            # os.environ (highest). External env overrides profile defaults.
            base_env = dict(getattr(cfg.profile, 'env', {}) or {})
            external = (
                dict(env) if env is not None else dict(os.environ)
            )
            env_map = {**base_env, **external}
            resolved = _deep_substitute(raw, cfg.vars, env_map)
            cfg = cls.from_dict(resolved)

        return cfg

    # -- Static Methods -- #

    @staticmethod
    def from_dict(
        raw: dict[str, Any],
    ) -> PipelineConfig:
        """
        Create a PipelineConfig instance from a dictionary.

        Parameters
        ----------
        raw : dict[str, Any]
            The raw dictionary to create the PipelineConfig from.

        Returns
        -------
        PipelineConfig
            The created PipelineConfig instance.
        """

        # Basic metadata
        name = raw.get('name')
        version = raw.get('version')

        # Profile and vars
        prof_raw = raw.get('profile', {}) or {}
        profile = ProfileConfig(
            default_target=prof_raw.get('default_target'),
            env={
                k: str(v)
                for k, v in (prof_raw.get('env', {}) or {}).items()
            },
        )
        vars_map: dict[str, Any] = dict(raw.get('vars', {}) or {})

        # APIs
        apis: dict[str, ApiConfig] = {}
        for api_name, api_obj in (raw.get('apis', {}) or {}).items():
            apis[str(api_name)] = ApiConfig.from_obj(api_obj)

        # Databases and file systems (pass-through structures)
        databases = dict(raw.get('databases', {}) or {})
        file_systems = dict(raw.get('file_systems', {}) or {})

        # Sources
        sources: list[Source] = []
        for s in (raw.get('sources', []) or []):
            if not isinstance(s, dict):
                continue
            stype = str(s.get('type', '')).casefold()
            sname = str(s.get('name')) if s.get('name') is not None else None
            if not sname:
                continue
            if stype == 'file':
                sources.append(
                    SourceFile(
                        name=sname,
                        type='file',
                        format=s.get('format'),
                        path=s.get('path'),
                        options=dict(s.get('options', {}) or {}),
                    ),
                )
            elif stype == 'database':
                sources.append(
                    SourceDb(
                        name=sname,
                        type='database',
                        connection_string=s.get('connection_string'),
                        query=s.get('query'),
                    ),
                )
            elif stype == 'api':
                # Allow either direct URL or reference to top-level API
                sources.append(
                    SourceApi(
                        name=sname,
                        type='api',
                        url=s.get('url'),
                        params=dict(s.get('params', {}) or {}),
                        headers={
                            k: str(v)
                            for k, v in (s.get('headers', {}) or {}).items()
                        },
                        pagination=PaginationConfig.from_obj(
                            s.get('pagination'),
                        ),
                        rate_limit=RateLimitConfig.from_obj(
                            s.get('rate_limit'),
                        ),
                        api=s.get('api') or s.get('service'),
                        endpoint=s.get('endpoint'),
                    ),
                )
            else:
                # Unknown type - skip gracefully
                continue

        # Validations/Transforms
        validations = dict(raw.get('validations', {}) or {})
        transforms = dict(raw.get('transforms', {}) or {})

        # Targets
        targets: list[Target] = []
        for t in (raw.get('targets', []) or []):
            if not isinstance(t, dict):
                continue
            ttype = str(t.get('type', '')).casefold()
            tname = str(t.get('name')) if t.get('name') is not None else None
            if not tname:
                continue
            if ttype == 'file':
                targets.append(
                    TargetFile(
                        name=tname,
                        type='file',
                        format=t.get('format'),
                        path=t.get('path'),
                    ),
                )
            elif ttype == 'api':
                targets.append(
                    TargetApi(
                        name=tname,
                        type='api',
                        url=t.get('url'),
                        method=t.get('method'),
                        headers={
                            k: str(v)
                            for k, v in (t.get('headers', {}) or {}).items()
                        },
                    ),
                )
            elif ttype == 'database':
                targets.append(
                    TargetDb(
                        name=tname,
                        type='database',
                        connection_string=t.get('connection_string'),
                        table=t.get('table'),
                        mode=t.get('mode'),
                    ),
                )
            else:
                continue

        # Jobs
        jobs: list[JobConfig] = []
        for j in (raw.get('jobs', []) or []):
            if not isinstance(j, dict):
                continue
            name = j.get('name')
            if not isinstance(name, str):
                continue
            # Extract
            ex_raw = j.get('extract') or {}
            extract = None
            if isinstance(ex_raw, dict) and ex_raw.get('source'):
                extract = ExtractRef(
                    source=str(ex_raw.get('source')),
                    options=dict(ex_raw.get('options', {}) or {}),
                )
            # Validate
            v_raw = j.get('validate') or {}
            validate = None
            if isinstance(v_raw, dict) and v_raw.get('ruleset'):
                validate = ValidationRef(
                    ruleset=str(v_raw.get('ruleset')),
                    severity=v_raw.get('severity'),
                    phase=v_raw.get('phase'),
                )
            # Transform
            tr_raw = j.get('transform') or {}
            transform = None
            if isinstance(tr_raw, dict) and tr_raw.get('pipeline'):
                transform = TransformRef(pipeline=str(tr_raw.get('pipeline')))
            # Load
            ld_raw = j.get('load') or {}
            load = None
            if isinstance(ld_raw, dict) and ld_raw.get('target'):
                load = LoadRef(
                    target=str(ld_raw.get('target')),
                    overrides=dict(ld_raw.get('overrides', {}) or {}),
                )

            jobs.append(
                JobConfig(
                    name=name,
                    description=j.get('description'),
                    extract=extract,
                    validate=validate,
                    transform=transform,
                    load=load,
                ),
            )

        return PipelineConfig(
            name=name,
            version=version,
            profile=profile,
            vars=vars_map,
            apis=apis,
            databases=databases,
            file_systems=file_systems,
            sources=sources,
            validations=validations,
            transforms=transforms,
            targets=targets,
            jobs=jobs,
        )


# SECTION: TYPE ALIASES ===================================================== #


type Source = SourceFile | SourceDb | SourceApi
type Target = TargetFile | TargetApi | TargetDb
