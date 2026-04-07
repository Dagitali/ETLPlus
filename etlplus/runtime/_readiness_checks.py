"""
:mod:`etlplus.runtime._readiness_checks` module.

Connector, dependency, and provider-environment readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any
from typing import cast
from urllib.parse import urlsplit

from .._config import Config
from ..connector import Connector
from ..connector import DataConnectorType
from ..storage import StorageScheme
from ._readiness_support import _AWS_ENV_HINTS
from ._readiness_support import _AZURE_STORAGE_BOOTSTRAP_ENV
from ._readiness_support import _AZURE_STORAGE_CREDENTIAL_ENV
from ._readiness_support import _FORMAT_EXTRA_REQUIREMENTS
from ._readiness_support import _SCHEME_EXTRA_REQUIREMENTS
from ._readiness_support import _RequirementSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'aws_env_hint_present',
    'azure_authority_has_account_host',
    'coerce_connector_storage_scheme',
    'coerce_storage_scheme',
    'connector_gap_guidance',
    'connector_gap_rows',
    'connector_readiness_checks',
    'connector_type',
    'connector_type_choices',
    'connector_type_guidance',
    'dedupe_rows',
    'explicit_aws_credential_gap',
    'iter_connectors',
    'missing_requirement_guidance',
    'missing_requirement_rows',
    'netcdf_available',
    'provider_environment_checks',
    'provider_environment_rows',
    'requirement_available',
    'requirement_row',
]


# SECTION: FUNCTIONS ======================================================== #


def aws_env_hint_present(
    env: Mapping[str, str],
) -> bool:
    """
    Return whether common AWS credential-chain env hints are present.

    The presence of any of these environment variables is a strong signal that
    the user intends to use AWS credentials, even if the variables are not
    sufficient for a fully explicit configuration. This is used to provide more
    targeted guidance when an S3 path is detected but no AWS credential hints
    are found in the environment. The check is intentionally broad to avoid
    false negatives for users relying on shared config files, container
    credentials, or instance metadata, which may not have a single specific
    environment variable set.

    Parameters
    ----------
    env : Mapping[str, str]
        The environment mapping to check for AWS credential hints, typically
        ``os.environ`` or a similar mapping.

    Returns
    -------
    bool
        ``True`` if any common AWS credential-chain environment variable is
        present, ``False`` if not.
    """
    return any(bool(env.get(name)) for name in _AWS_ENV_HINTS)


def azure_authority_has_account_host(
    path: str,
) -> bool:
    """
    Return whether one Azure path authority embeds an account host.

    For Azure storage paths, the authority component of the URI may include an
    account host, which can serve as a bootstrap credential hint. For example,
    in the path ``https://myaccount.blob.core.windows.net/mycontainer/myblob``,
    the authority component is ``myaccount.blob.core.windows.net``.

    Parameters
    ----------
    path : str
        The path to check for an Azure storage authority with an account host.

    Returns
    -------
    bool
        ``True`` if the authority component of the Azure storage path includes
        an account host, ``False`` if not.

    """
    authority = urlsplit(path).netloc
    _, separator, account_host = authority.partition('@')
    return bool(separator and account_host)


def coerce_connector_storage_scheme(
    value: str,
) -> str | None:
    """
    Return one normalized storage scheme from raw connector-type text.

    Parameters
    ----------
    value : str
        The raw connector type string to coerce as a storage scheme.

    Returns
    -------
    str | None
        The normalized storage scheme name if coercion is successful, or
        ``None`` if the value cannot be coerced as a known storage scheme.
    """
    if not value:
        return None
    try:
        return str(StorageScheme.coerce(value))
    except ValueError:
        return None


def coerce_storage_scheme(
    path: str,
) -> str | None:
    """
    Return one normalized storage scheme for *path* when present.

    Parameters
    ----------
    path : str
        The path to check for a storage scheme.

    Returns
    -------
    str | None
        The normalized storage scheme name if present, or ``None`` if not.
    """
    if '://' not in path:
        return None
    parsed = urlsplit(path)
    if not parsed.scheme:
        return None
    try:
        return str(StorageScheme.coerce(parsed.scheme))
    except ValueError:
        return parsed.scheme.lower()


def connector_gap_guidance(
    *,
    api_reference: str | None = None,
    issue: str,
) -> str | None:
    """
    Return one actionable guidance string for a blocking connector gap.

    Parameters
    ----------
    api_reference : str | None
        The API reference associated with the connector gap, if any.
    issue : str
        The specific issue causing the connector gap.

    Returns
    -------
    str | None
        An actionable guidance string if the issue is recognized, or ``None``
        if not.
    """
    match issue:
        case 'missing path':
            return 'Set "path" to a local path or storage URI for this file connector.'
        case 'missing url or api reference':
            return (
                'Set "url" to a reachable endpoint or "api" to a configured '
                'top-level API name.'
            )
        case 'missing connection_string':
            return 'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
        case issue_text if issue_text.startswith('unknown api reference: '):
            if api_reference:
                return (
                    f'Define "{api_reference}" under top-level "apis" or update '
                    'the connector "api" reference.'
                )
            return 'Define the referenced API under top-level "apis".'
        case _:
            return None


def dedupe_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Return rows with duplicates removed while preserving order.

    Parameters
    ----------
    rows : list[dict[str, Any]]
        The list of rows to deduplicate.

    Returns
    -------
    list[dict[str, Any]]
        The deduplicated list of rows.
    """
    unique_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        key = (
            row['connector'],
            row['role'],
            row['missing_package'],
            row['reason'],
            row['extra'],
        )
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def iter_connectors(
    cfg: Config,
) -> Iterator[tuple[str, Connector]]:
    """
    Yield source and target connectors tagged with their role.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.

    Returns
    -------
    Iterator[tuple[str, Connector]]
        An iterator over tuples of role and connector.

    Yields
    ------
    tuple[str, Connector]
        Tuples of role ("source" or "target") and connector objects from the
        configuration.
    """
    yield from (('source', connector) for connector in cfg.sources)
    yield from (('target', connector) for connector in cfg.targets)


def missing_requirement_guidance(
    *,
    detected_format: str | None = None,
    detected_scheme: str | None = None,
    package: str,
    extra: str | None,
) -> str:
    """
    Return one actionable remediation string for a missing dependency.

    Parameters
    ----------
    detected_format : str | None
        The detected file format, if any.
    detected_scheme : str | None
        The detected storage scheme, if any.
    package : str
        The name of the missing package.
    extra : str | None
        The name of the ETLPlus extra that includes the missing package, if any.

    Returns
    -------
    str
        An actionable remediation string for the missing dependency.
    """
    install_hint = (
        f'Install {package} directly or install the ETLPlus "{extra}" extra.'
        if extra
        else f'Install {package}.'
    )
    if detected_format == 'nc':
        return (
            'Install xarray plus one of netCDF4 or h5netcdf, or install the '
            'ETLPlus "file" extra.'
        )
    if detected_format is not None:
        return f'{install_hint} Required for "{detected_format}" file format.'
    if detected_scheme is not None:
        return f'{install_hint} Required for "{detected_scheme}" storage paths.'
    return install_hint


def connector_gap_rows(
    cfg: Config,
) -> list[dict[str, Any]]:
    """
    Return connector configuration gaps that will block execution.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries representing the configuration gaps.
    """
    gaps: list[dict[str, Any]] = []
    for role, connector in iter_connectors(cfg):
        connector_name = str(getattr(connector, 'name', '<unnamed>'))
        connector_type_name = str(getattr(connector, 'type', ''))
        coerced_type = connector_type(connector_type_name)

        if coerced_type is None:
            gaps.append(
                {
                    'connector': connector_name,
                    'guidance': connector_type_guidance(connector_type_name),
                    'issue': 'unsupported type',
                    'role': role,
                    'supported_types': list(connector_type_choices()),
                    'type': connector_type_name,
                },
            )
            continue

        if coerced_type == DataConnectorType.FILE:
            if not getattr(connector, 'path', None):
                gaps.append(
                    {
                        'connector': connector_name,
                        'guidance': connector_gap_guidance(issue='missing path'),
                        'issue': 'missing path',
                        'role': role,
                        'type': connector_type_name,
                    },
                )
            continue

        if coerced_type == DataConnectorType.API:
            url = getattr(connector, 'url', None)
            api_ref = getattr(connector, 'api', None)
            if not url and not api_ref:
                gaps.append(
                    {
                        'connector': connector_name,
                        'guidance': connector_gap_guidance(
                            issue='missing url or api reference',
                        ),
                        'issue': 'missing url or api reference',
                        'role': role,
                        'type': connector_type_name,
                    },
                )
            elif api_ref and api_ref not in cfg.apis:
                gaps.append(
                    {
                        'connector': connector_name,
                        'guidance': connector_gap_guidance(
                            api_reference=cast(str, api_ref),
                            issue=f'unknown api reference: {api_ref}',
                        ),
                        'issue': f'unknown api reference: {api_ref}',
                        'role': role,
                        'type': connector_type_name,
                    },
                )
            continue

        if coerced_type == DataConnectorType.DATABASE:
            if not getattr(connector, 'connection_string', None):
                gaps.append(
                    {
                        'connector': connector_name,
                        'guidance': connector_gap_guidance(
                            issue='missing connection_string',
                        ),
                        'issue': 'missing connection_string',
                        'role': role,
                        'type': connector_type_name,
                    },
                )
            continue

    return gaps


def connector_readiness_checks(
    cfg: Config,
    *,
    connector_gap_rows_fn: Callable[[Config], list[dict[str, Any]]],
    make_check: Callable[..., dict[str, Any]],
    missing_requirement_rows_fn: Callable[[Config], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    Return connector configuration and dependency readiness checks.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    connector_gap_rows_fn : Callable[[Config], list[dict[str, Any]]]
        A function that returns a list of connector configuration gaps.
    make_check : Callable[..., dict[str, Any]]
        A function that creates a readiness check dictionary.
    missing_requirement_rows_fn : Callable[[Config], list[dict[str, Any]]]
        A function that returns a list of missing optional dependencies.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries representing the readiness checks.
    """
    checks: list[dict[str, Any]] = []
    gaps = connector_gap_rows_fn(cfg)
    if gaps:
        checks.append(
            make_check(
                'connector-readiness',
                'error',
                (
                    'One or more configured connectors are missing required '
                    'runtime fields or use unsupported connector types.'
                ),
                gaps=gaps,
            ),
        )
    else:
        checks.append(
            make_check(
                'connector-readiness',
                'ok',
                'Configured connectors include the required runtime fields.',
            ),
        )

    missing_requirements = missing_requirement_rows_fn(cfg)
    if missing_requirements:
        checks.append(
            make_check(
                'optional-dependencies',
                'error',
                (
                    'Configured connectors require optional dependencies that '
                    'are not installed.'
                ),
                missing_requirements=missing_requirements,
            ),
        )
    else:
        checks.append(
            make_check(
                'optional-dependencies',
                'ok',
                (
                    'No missing optional dependencies were detected for '
                    'configured connectors.'
                ),
            ),
        )
    return checks


def connector_type(
    connector_type_str: str,
) -> DataConnectorType | None:
    """
    Return one coerced connector type or ``None`` when unsupported.

    Parameters
    ----------
    connector_type_str : str
        The connector type string to coerce.

    Returns
    -------
    DataConnectorType | None
        The coerced connector type or ``None`` if unsupported.
    """
    try:
        return DataConnectorType.coerce(connector_type_str)
    except ValueError:
        return None


def connector_type_choices() -> tuple[str, ...]:
    """
    Return the supported connector type names.

    Returns
    -------
    tuple[str, ...]
        A tuple of supported connector type names.
    """
    return tuple(str(member.value) for member in DataConnectorType)


def connector_type_guidance(
    connector_type_str: str,
) -> str:
    """
    Return actionable guidance for an unsupported connector type.

    Parameters
    ----------
    connector_type_str : str
        The connector type string to provide guidance for.

    Returns
    -------
    str
        Actionable guidance for the unsupported connector type.
    """
    supported = ', '.join(connector_type_choices())
    normalized = connector_type_str.strip().lower()
    if not normalized:
        return f'Set type to one of: {supported}.'

    if coerce_connector_storage_scheme(normalized) is not None:
        return (
            f'"{normalized}" is a storage scheme, not a connector type. '
            'Use connector type "file" and keep the provider in the path '
            'or URI scheme.'
        )
    return f'Use one of the supported connector types: {supported}.'


def explicit_aws_credential_gap(
    env: Mapping[str, str],
) -> dict[str, Any] | None:
    """
    Return one AWS env error row for incomplete explicit credentials.

    This check looks for the presence of partial explicit AWS credential
    environment variables that indicate an attempt at explicit credential
    configuration, but the variables are not sufficient for a complete explicit
    configuration. The check is intentionally specific to avoid false positives
    for users who are not attempting explicit credential configuration, as the
    presence of any of these variables is a strong signal of that intent. If
    both ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` are not set, but
    ``AWS_SESSION_TOKEN`` is set, it indicates an incomplete configuration.

    Parameters
    ----------
    env : Mapping[str, str]
        The environment variables to check.

    Returns
    -------
    dict[str, Any] | None
        An error row for incomplete explicit AWS credentials, or ``None`` if no
        issues are found.
    """
    access_key = bool(env.get('AWS_ACCESS_KEY_ID'))
    secret_key = bool(env.get('AWS_SECRET_ACCESS_KEY'))
    session_token = bool(env.get('AWS_SESSION_TOKEN'))
    if access_key and secret_key:
        return None
    if not (access_key or secret_key or session_token):
        return None

    missing_env: list[str] = []
    if not access_key:
        missing_env.append('AWS_ACCESS_KEY_ID')
    if not secret_key:
        missing_env.append('AWS_SECRET_ACCESS_KEY')
    return {
        'guidance': (
            'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
            'remove the partial explicit credential env vars and rely on '
            'AWS_PROFILE, shared config files, container credentials, or '
            'instance metadata.'
        ),
        'missing_env': missing_env,
        'provider': 'aws-s3',
        'reason': (
            'Incomplete explicit AWS access-key configuration was detected '
            'for this S3 path.'
        ),
        'severity': 'error',
    }


def missing_requirement_rows(
    *,
    cfg: Config,
    netcdf_available_fn: Callable[[], bool],
    requirement_available_fn: Callable[[_RequirementSpec], bool],
) -> list[dict[str, Any]]:
    """
    Return missing optional dependency rows for configured connectors.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    netcdf_available_fn : Callable[[], bool]
        A function that returns whether netCDF support dependencies are installed.
    requirement_available_fn : Callable[[_RequirementSpec], bool]
        A function that returns whether the dependencies for a given
        requirement are installed.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries representing the missing optional dependencies
        for the configured connectors.
    """
    rows: list[dict[str, Any]] = []
    for role, connector in iter_connectors(cfg):
        connector_name = str(getattr(connector, 'name', '<unnamed>'))
        path = getattr(connector, 'path', None)
        format_name = str(getattr(connector, 'format', '') or '').lower()

        if path:
            scheme = coerce_storage_scheme(path)
            requirement = _SCHEME_EXTRA_REQUIREMENTS.get(scheme or '')
            if scheme and requirement and not requirement_available_fn(requirement):
                rows.append(
                    requirement_row(
                        connector=connector_name,
                        detected_scheme=scheme,
                        reason=f'{scheme} storage path requires {requirement.package}',
                        requirement=requirement,
                        role=role,
                    ),
                )

        if format_name == 'nc':
            if not netcdf_available_fn():
                rows.append(
                    {
                        'connector': connector_name,
                        'detected_format': 'nc',
                        'extra': 'file',
                        'guidance': missing_requirement_guidance(
                            detected_format='nc',
                            package='xarray/netCDF4',
                            extra='file',
                        ),
                        'missing_package': 'xarray/netCDF4',
                        'reason': 'nc format requires xarray and netCDF4 or h5netcdf',
                        'role': role,
                    },
                )
            continue

        requirement = _FORMAT_EXTRA_REQUIREMENTS.get(format_name)
        if requirement and not requirement_available_fn(requirement):
            rows.append(
                requirement_row(
                    connector=connector_name,
                    detected_format=format_name,
                    reason=f'{format_name} format requires {requirement.package}',
                    requirement=requirement,
                    role=role,
                ),
            )

    return dedupe_rows(rows)


def provider_environment_checks(
    *,
    cfg: Config,
    env: Mapping[str, str],
    make_check: Callable[..., dict[str, Any]],
    provider_environment_rows_fn: Callable[
        [Config, Mapping[str, str]],
        list[dict[str, Any]],
    ],
) -> list[dict[str, Any]]:
    """
    Return provider-specific environment readiness checks.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    env : Mapping[str, str]
        The environment variables to check for provider-specific gaps.
    make_check : Callable[..., dict[str, Any]]
        A function that creates a readiness check dictionary.
    provider_environment_rows_fn : Callable[
        [Config, Mapping[str, str]],
        list[dict[str, Any]],
    ]
        A function that returns a list of provider-specific environment gaps.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries representing the provider-specific environment
        readiness checks.
    """
    rows = provider_environment_rows_fn(cfg, env)
    if not rows:
        return [
            make_check(
                'provider-environment',
                'ok',
                'No provider-specific environment gaps were detected.',
            ),
        ]

    errors = sum(1 for row in rows if row['severity'] == 'error')
    warnings = sum(1 for row in rows if row['severity'] == 'warn')
    has_error = errors > 0
    return [
        make_check(
            'provider-environment',
            'error' if has_error else 'warn',
            (
                f'Provider environment gaps: {errors} error(s), {warnings} warning(s).'
                if has_error
                else f'Provider environment warnings: {warnings}.'
            ),
            environment_gaps=rows,
        ),
    ]


def provider_environment_rows(
    *,
    cfg: Config,
    env: Mapping[str, str],
) -> list[dict[str, Any]]:
    """
    Return provider-specific environment gaps for configured connectors.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    env : Mapping[str, str]
        The environment variables to check for provider-specific gaps.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries representing the provider-specific environment
        gaps.
    """
    rows: list[dict[str, Any]] = []
    azure_connection_string = bool(env.get('AZURE_STORAGE_CONNECTION_STRING'))
    azure_account_url = bool(env.get('AZURE_STORAGE_ACCOUNT_URL'))
    azure_credential = bool(env.get(_AZURE_STORAGE_CREDENTIAL_ENV))
    has_aws_hints = aws_env_hint_present(env)

    for role, connector in iter_connectors(cfg):
        connector_name = str(getattr(connector, 'name', '<unnamed>'))
        path = getattr(connector, 'path', None)
        if not isinstance(path, str) or not path:
            continue

        scheme = coerce_storage_scheme(path)
        match scheme:
            case 'azure-blob' | 'abfs':
                authority_has_account_host = azure_authority_has_account_host(path)
                if not (
                    azure_connection_string
                    or azure_account_url
                    or authority_has_account_host
                ):
                    rows.append(
                        {
                            'connector': connector_name,
                            'guidance': (
                                'Set AZURE_STORAGE_CONNECTION_STRING, set '
                                'AZURE_STORAGE_ACCOUNT_URL, or include the '
                                'account host in the path authority.'
                            ),
                            'missing_env': list(_AZURE_STORAGE_BOOTSTRAP_ENV),
                            'provider': 'azure-storage',
                            'reason': (
                                f'{scheme} path does not provide an account host '
                                'and no Azure storage bootstrap settings were found.'
                            ),
                            'role': role,
                            'scheme': scheme,
                            'severity': 'error',
                        },
                    )
                    continue

                if not azure_connection_string and not azure_credential:
                    rows.append(
                        {
                            'connector': connector_name,
                            'guidance': (
                                'Set AZURE_STORAGE_CREDENTIAL when the target is '
                                'not public, or use AZURE_STORAGE_CONNECTION_STRING '
                                'for a fully explicit configuration.'
                            ),
                            'missing_env': [_AZURE_STORAGE_CREDENTIAL_ENV],
                            'provider': 'azure-storage',
                            'reason': (
                                f'{scheme} access has no explicit Azure credential '
                                'configured; runtime access will only work for '
                                'public resources or other ambient authentication '
                                'handled by the SDK call site.'
                            ),
                            'role': role,
                            'scheme': scheme,
                            'severity': 'warn',
                        },
                    )
            case 's3':
                explicit_gap = explicit_aws_credential_gap(env)
                if explicit_gap:
                    rows.append(
                        {'connector': connector_name, 'role': role, 'scheme': scheme}
                        | explicit_gap,
                    )
                    continue
                if not has_aws_hints:
                    rows.append(
                        {
                            'connector': connector_name,
                            'guidance': (
                                'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                                'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                                'files, container credentials, or instance metadata.'
                            ),
                            'missing_env': list(_AWS_ENV_HINTS),
                            'provider': 'aws-s3',
                            'reason': (
                                'No common AWS credential-chain environment hints '
                                'were detected for this S3 path.'
                            ),
                            'role': role,
                            'scheme': scheme,
                            'severity': 'warn',
                        },
                    )
    return rows


def netcdf_available(
    *,
    package_available: Callable[[str], bool],
) -> bool:
    """
    Return whether netCDF support dependencies are installed.

    Parameters
    ----------
    package_available : Callable[[str], bool]
        A function that checks if a package is available.

    Returns
    -------
    bool
        True if netCDF support dependencies are installed, False otherwise.
    """
    return package_available('xarray') and (
        package_available('netCDF4') or package_available('h5netcdf')
    )


def requirement_available(
    requirement: _RequirementSpec,
    *,
    package_available: Callable[[str], bool],
) -> bool:
    """
    Return whether any module for one requirement is importable.

    Parameters
    ----------
    requirement : _RequirementSpec
        The requirement specification containing the modules to check.
    package_available : Callable[[str], bool]
        A function that checks if a package is available.

    Returns
    -------
    bool
        True if any module for the requirement is importable, False otherwise.
    """
    return any(package_available(module_name) for module_name in requirement.modules)


def requirement_row(
    *,
    connector: str,
    detected_format: str | None = None,
    detected_scheme: str | None = None,
    reason: str,
    requirement: _RequirementSpec,
    role: str,
) -> dict[str, Any]:
    """
    Return one missing-requirement row.

    Parameters
    ----------
    connector : str
        The name of the connector.
    detected_format : str | None, optional
        The detected format, if any.
    detected_scheme : str | None, optional
        The detected scheme, if any.
    reason : str
        The reason for the missing requirement.
    requirement : _RequirementSpec
        The requirement specification.
    role : str
        The role associated with the requirement.

    Returns
    -------
    dict[str, Any]
        A dictionary representing the missing requirement row.
    """
    row: dict[str, Any] = {
        'connector': connector,
        'extra': requirement.extra or '',
        'guidance': missing_requirement_guidance(
            detected_format=detected_format,
            detected_scheme=detected_scheme,
            package=requirement.package,
            extra=requirement.extra,
        ),
        'missing_package': requirement.package,
        'reason': reason,
        'role': role,
    }
    if detected_format is not None:
        row['detected_format'] = detected_format
    if detected_scheme is not None:
        row['detected_scheme'] = detected_scheme
    return row
