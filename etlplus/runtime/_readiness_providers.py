"""
:mod:`etlplus.runtime._readiness_providers` module.

Provider-specific runtime readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from .._config import Config
from ._readiness_connectors import coerce_storage_scheme
from ._readiness_connectors import iter_connectors
from ._readiness_support import _AWS_ENV_HINTS
from ._readiness_support import _AZURE_STORAGE_BOOTSTRAP_ENV
from ._readiness_support import _AZURE_STORAGE_CREDENTIAL_ENV

# SECTION: INTERNAL TYPE ALIASES ============================================ #


type _ProviderEnvironmentRowsFn = Callable[
    [Config, Mapping[str, str]],
    list[_ReadinessRow],
]
type _ReadinessRow = dict[str, Any]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _provider_gap_row(
    *,
    connector: str,
    guidance: str,
    missing_env: list[str],
    provider: str,
    reason: str,
    role: str,
    scheme: str,
    severity: str,
) -> _ReadinessRow:
    """Return one normalized provider-environment gap row."""
    return {
        'connector': connector,
        'guidance': guidance,
        'missing_env': missing_env,
        'provider': provider,
        'reason': reason,
        'role': role,
        'scheme': scheme,
        'severity': severity,
    }


def _azure_provider_gaps(
    *,
    connector: str,
    path: str,
    role: str,
    scheme: str,
    azure_account_url: bool,
    azure_connection_string: bool,
    azure_credential: bool,
) -> list[_ReadinessRow]:
    """Return Azure bootstrap or credential gaps for one connector path."""
    authority_has_account_host = azure_authority_has_account_host(path)
    if not (azure_connection_string or azure_account_url or authority_has_account_host):
        return [
            _provider_gap_row(
                connector=connector,
                guidance=(
                    'Set AZURE_STORAGE_CONNECTION_STRING, set '
                    'AZURE_STORAGE_ACCOUNT_URL, or include the '
                    'account host in the path authority.'
                ),
                missing_env=list(_AZURE_STORAGE_BOOTSTRAP_ENV),
                provider='azure-storage',
                reason=(
                    f'{scheme} path does not provide an account host '
                    'and no Azure storage bootstrap settings were found.'
                ),
                role=role,
                scheme=scheme,
                severity='error',
            ),
        ]

    if azure_connection_string or azure_credential:
        return []

    return [
        _provider_gap_row(
            connector=connector,
            guidance=(
                'Set AZURE_STORAGE_CREDENTIAL when the target is '
                'not public, or use AZURE_STORAGE_CONNECTION_STRING '
                'for a fully explicit configuration.'
            ),
            missing_env=[_AZURE_STORAGE_CREDENTIAL_ENV],
            provider='azure-storage',
            reason=(
                f'{scheme} access has no explicit Azure credential '
                'configured; runtime access will only work for '
                'public resources or other ambient authentication '
                'handled by the SDK call site.'
            ),
            role=role,
            scheme=scheme,
            severity='warn',
        ),
    ]


def _s3_provider_gaps(
    *,
    connector: str,
    env: Mapping[str, str],
    has_aws_hints: bool,
    role: str,
) -> list[_ReadinessRow]:
    """Return AWS provider gaps for one S3 connector path."""
    explicit_gap = explicit_aws_credential_gap(env)
    if explicit_gap:
        return [{'connector': connector, 'role': role, 'scheme': 's3'} | explicit_gap]
    if has_aws_hints:
        return []
    return [
        _provider_gap_row(
            connector=connector,
            guidance=(
                'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                'files, container credentials, or instance metadata.'
            ),
            missing_env=list(_AWS_ENV_HINTS),
            provider='aws-s3',
            reason=(
                'No common AWS credential-chain environment hints '
                'were detected for this S3 path.'
            ),
            role=role,
            scheme='s3',
            severity='warn',
        ),
    ]


# SECTION: FUNCTIONS ======================================================== #


def aws_env_hint_present(
    env: Mapping[str, str],
) -> bool:
    """
    Return whether common AWS credential-chain *env* hints are present.

    This is a heuristic to determine whether AWS credentials may be configured
    in the environment for SDK credential providers like profiles, shared
    config files, container credentials, or instance metadata. The presence of
    any of these environment variables is a strong signal that the user intends
    to use AWS credentials, even if the variables are not sufficient for a fully
    explicit configuration. This is used to provide more targeted guidance when
    an S3 path is detected but no AWS credential hints are found in the
    environment. The check is intentionally broad to avoid false negatives for
    users relying on shared config files, container credentials, or instance
    metadata, which may not have a single specific environment variable set.

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


def azure_authority_has_account_host(path: str) -> bool:
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


def explicit_aws_credential_gap(
    env: Mapping[str, str],
) -> _ReadinessRow | None:
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
    _ReadinessRow | None
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


def provider_environment_checks(
    *,
    cfg: Config,
    env: Mapping[str, str],
    make_check: Callable[..., dict[str, Any]],
    provider_environment_rows_fn: _ProviderEnvironmentRowsFn,
) -> list[_ReadinessRow]:
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
    provider_environment_rows_fn : _ProviderEnvironmentRowsFn
        A function that returns a list of provider-specific environment gaps.

    Returns
    -------
    list[_ReadinessRow]
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
) -> list[_ReadinessRow]:
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
    list[_ReadinessRow]
        A list of dictionaries representing the provider-specific environment
        gaps.
    """
    rows: list[_ReadinessRow] = []
    azure_connection_string = bool(env.get('AZURE_STORAGE_CONNECTION_STRING'))
    azure_account_url = bool(env.get('AZURE_STORAGE_ACCOUNT_URL'))
    azure_credential = bool(env.get(_AZURE_STORAGE_CREDENTIAL_ENV))
    has_aws_hints = aws_env_hint_present(env)

    for role, connector in iter_connectors(cfg):
        connector_name = str(getattr(connector, 'name', '<unnamed>'))
        path = getattr(connector, 'path', None)
        if not isinstance(path, str) or not path:
            continue

        match coerce_storage_scheme(path):
            case 'azure-blob' | 'abfs' as scheme:
                rows.extend(
                    _azure_provider_gaps(
                        connector=connector_name,
                        path=path,
                        role=role,
                        scheme=scheme,
                        azure_account_url=azure_account_url,
                        azure_connection_string=azure_connection_string,
                        azure_credential=azure_credential,
                    ),
                )
            case 's3':
                rows.extend(
                    _s3_provider_gaps(
                        connector=connector_name,
                        env=env,
                        has_aws_hints=has_aws_hints,
                        role=role,
                    ),
                )
            case _:
                continue
    return rows
