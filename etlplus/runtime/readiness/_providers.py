"""
:mod:`etlplus.runtime.readiness._providers` module.

Provider-specific runtime readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import TypedDict
from urllib.parse import urlsplit

from ..._config import Config
from ...utils._types import IssueSeverity
from ._base import ReadinessSupportPolicy
from ._support import AWS_ENV_HINTS
from ._support import AZURE_STORAGE_BOOTSTRAP_ENV
from ._support import AZURE_STORAGE_CREDENTIAL_ENV
from ._support import ReadinessRow

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ProviderEnvironmentPolicy',
]


# SECTION: INTERNAL TYPED DICTS ============================================= #


class _ProviderGapDetails(TypedDict):
    """Provider-gap fields before connector, role, and scheme are attached."""

    guidance: str
    missing_env: list[str]
    provider: str
    reason: str
    severity: IssueSeverity


class _ProviderGapRow(_ProviderGapDetails):
    """Normalized provider-environment gap row."""

    connector: str
    role: str
    scheme: str


type _ProviderEnvironmentRowsFn = Callable[
    [Config, Mapping[str, str]],
    list[_ProviderGapRow],
]


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _ResolvedConnectorPath:
    """Normalized connector path state reused by provider checks."""

    connector: str
    path: str
    role: str
    scheme: str


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _aws_env_hint_present(
    env: Mapping[str, str],
) -> bool:
    """Return whether common AWS credential-chain *env* hints are present."""
    return any(bool(env.get(name)) for name in AWS_ENV_HINTS)


def _azure_authority_has_account_host(path: str) -> bool:
    """Return whether one Azure path authority embeds an account host."""
    authority = urlsplit(path).netloc
    _, separator, account_host = authority.partition('@')
    return bool(separator and account_host)


def _provider_gap_from_details(
    *,
    connector: str,
    details: _ProviderGapDetails,
    role: str,
    scheme: str,
) -> _ProviderGapRow:
    """Attach connector context to provider-gap details."""
    return _ProviderGapRow(
        connector=connector,
        guidance=details['guidance'],
        missing_env=details['missing_env'],
        provider=details['provider'],
        reason=details['reason'],
        role=role,
        scheme=scheme,
        severity=details['severity'],
    )


def _iter_connector_paths(
    cfg: Config,
) -> tuple[_ResolvedConnectorPath, ...]:
    """Return normalized connector-path rows for provider policy checks."""
    return tuple(
        _ResolvedConnectorPath(
            connector=str(getattr(connector, 'name', '<unnamed>')),
            path=path,
            role=role,
            scheme=scheme,
        )
        for role, connector in ReadinessSupportPolicy.iter_connectors(cfg)
        if isinstance(path := getattr(connector, 'path', None), str)
        and bool(path)
        and (scheme := ReadinessSupportPolicy.coerce_storage_scheme(path)) is not None
    )


def _azure_provider_gaps(
    *,
    connector: str,
    path: str,
    role: str,
    scheme: str,
    azure_account_url: bool,
    azure_connection_string: bool,
    azure_credential: bool,
) -> list[_ProviderGapRow]:
    """Return Azure bootstrap or credential gaps for one connector path."""
    authority_has_account_host = _azure_authority_has_account_host(path)
    if not (azure_connection_string or azure_account_url or authority_has_account_host):
        return [
            _ProviderGapRow(
                connector=connector,
                guidance=(
                    'Set AZURE_STORAGE_CONNECTION_STRING, set '
                    'AZURE_STORAGE_ACCOUNT_URL, or include the '
                    'account host in the path authority.'
                ),
                missing_env=list(AZURE_STORAGE_BOOTSTRAP_ENV),
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
        _ProviderGapRow(
            connector=connector,
            guidance=(
                'Set AZURE_STORAGE_CREDENTIAL when the target is '
                'not public, or use AZURE_STORAGE_CONNECTION_STRING '
                'for a fully explicit configuration.'
            ),
            missing_env=[AZURE_STORAGE_CREDENTIAL_ENV],
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
) -> list[_ProviderGapRow]:
    """Return AWS provider gaps for one S3 connector path."""
    explicit_gap = ProviderEnvironmentPolicy.explicit_aws_credential_gap(env)
    if explicit_gap:
        return [
            _provider_gap_from_details(
                connector=connector,
                details=explicit_gap,
                role=role,
                scheme='s3',
            ),
        ]
    if has_aws_hints:
        return []
    return [
        _ProviderGapRow(
            connector=connector,
            guidance=(
                'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                'files, container credentials, or instance metadata.'
            ),
            missing_env=list(AWS_ENV_HINTS),
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


class ProviderEnvironmentPolicy:
    """Evaluate provider-specific environment readiness for file connectors."""

    # -- Static Methods -- #

    @staticmethod
    def explicit_aws_credential_gap(
        env: Mapping[str, str],
    ) -> _ProviderGapDetails | None:
        """
        Return one AWS env error row for incomplete explicit credentials.

        This check looks for the presence of partial explicit AWS credential
        environment variables that indicate an attempt at explicit credential
        configuration, but the variables are not sufficient for a complete
        explicit configuration. The check is intentionally specific to avoid
        false positives for users who are not attempting explicit credential
        configuration, as the presence of any of these variables is a strong
        signal of that intent. If both ``AWS_ACCESS_KEY_ID`` and
        ``AWS_SECRET_ACCESS_KEY`` are not set, but ``AWS_SESSION_TOKEN`` is
        set, it indicates an incomplete configuration.

        Parameters
        ----------
        env : Mapping[str, str]
            The environment variables to check.

        Returns
        -------
        _ProviderGapDetails | None
            An error row for incomplete explicit AWS credentials, or ``None``
            if no issues are found.
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

    @staticmethod
    def environment_checks(
        *,
        cfg: Config,
        env: Mapping[str, str],
        make_check: Callable[..., dict[str, Any]],
        provider_environment_rows_fn: _ProviderEnvironmentRowsFn,
    ) -> list[ReadinessRow]:
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
            A function that returns a list of provider-specific environment
            gaps.

        Returns
        -------
        list[ReadinessRow]
            A list of dictionaries representing the provider-specific
            environment readiness checks.
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
                    'Provider environment gaps: '
                    f'{errors} error(s), {warnings} warning(s).'
                    if has_error
                    else f'Provider environment warnings: {warnings}.'
                ),
                environment_gaps=rows,
            ),
        ]

    @staticmethod
    def environment_rows(
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[_ProviderGapRow]:
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
        list[_ProviderGapRow]
            A list of dictionaries representing the provider-specific
            environment gaps.
        """
        rows: list[_ProviderGapRow] = []
        azure_connection_string = bool(env.get('AZURE_STORAGE_CONNECTION_STRING'))
        azure_account_url = bool(env.get('AZURE_STORAGE_ACCOUNT_URL'))
        azure_credential = bool(env.get(AZURE_STORAGE_CREDENTIAL_ENV))
        has_aws_hints = _aws_env_hint_present(env)

        for resolved in _iter_connector_paths(cfg):
            match resolved.scheme:
                case 'azure-blob' | 'abfs' as scheme:
                    rows.extend(
                        _azure_provider_gaps(
                            connector=resolved.connector,
                            path=resolved.path,
                            role=resolved.role,
                            scheme=scheme,
                            azure_account_url=azure_account_url,
                            azure_connection_string=azure_connection_string,
                            azure_credential=azure_credential,
                        ),
                    )
                case 's3':
                    rows.extend(
                        _s3_provider_gaps(
                            connector=resolved.connector,
                            env=env,
                            has_aws_hints=has_aws_hints,
                            role=resolved.role,
                        ),
                    )
                case _:
                    continue
        return rows
