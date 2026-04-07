"""
:mod:`etlplus.runtime._readiness` module.

Runtime readiness checks for the CLI and future execution surfaces.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path
from typing import Any
from typing import Final
from typing import Literal
from typing import cast
from urllib.parse import urlsplit

from .. import __version__
from .._config import Config
from ..connector import Connector
from ..connector import DataConnectorType
from ..connector import parse_connector
from ..file import File
from ..file import FileFormat
from ..storage import StorageScheme
from ..utils import deep_substitute
from ..utils import maybe_mapping
from ..utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]


# SECTION: TYPE ALIASES ===================================================== #


type CheckStatus = Literal['ok', 'warn', 'error', 'skipped']


# SECTION: INTERNAL DATA CLASSES ============================================== #


@dataclass(frozen=True, slots=True)
class _RequirementSpec:
    """One optional runtime dependency requirement."""

    # -- Instance Attributes -- #

    modules: tuple[str, ...]
    package: str
    extra: str | None = None


@dataclass(frozen=True, slots=True)
class _ResolvedConfigContext:
    """Resolved config state reused across config readiness checks."""

    # -- Instance Attributes -- #

    raw: StrAnyMap
    effective_env: dict[str, str]
    unresolved_tokens: list[str]
    resolved_raw: StrAnyMap
    resolved_cfg: Config | None


# SECTION: INTERNAL CONSTANTS =============================================== #


_AWS_ENV_HINTS: Final[tuple[str, ...]] = (
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SESSION_TOKEN',
    'AWS_PROFILE',
    'AWS_DEFAULT_PROFILE',
    'AWS_ROLE_ARN',
    'AWS_WEB_IDENTITY_TOKEN_FILE',
    'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
    'AWS_CONTAINER_CREDENTIALS_FULL_URI',
    'AWS_SHARED_CREDENTIALS_FILE',
    'AWS_CONFIG_FILE',
)
_AZURE_STORAGE_BOOTSTRAP_ENV: Final[tuple[str, ...]] = (
    'AZURE_STORAGE_CONNECTION_STRING',
    'AZURE_STORAGE_ACCOUNT_URL',
)
_AZURE_STORAGE_CREDENTIAL_ENV: Final[str] = 'AZURE_STORAGE_CREDENTIAL'


_SUPPORTED_PYTHON_RANGE: Final[tuple[tuple[int, int], tuple[int, int]]] = (
    (3, 13),
    (3, 15),
)
_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r'\$\{([^}]+)\}')


_FORMAT_EXTRA_REQUIREMENTS: Final[dict[str, _RequirementSpec]] = {
    'dta': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'hdf5': _RequirementSpec(('tables',), 'tables'),
    'rda': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'rds': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'sav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'zsav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
}
_SCHEME_EXTRA_REQUIREMENTS: Final[dict[str, _RequirementSpec]] = {
    'abfs': _RequirementSpec(
        ('azure.storage.filedatalake',),
        'azure-storage-file-datalake',
        'storage',
    ),
    'azure-blob': _RequirementSpec(
        ('azure.storage.blob',),
        'azure-storage-blob',
        'storage',
    ),
    's3': _RequirementSpec(('boto3',), 'boto3', 'storage'),
}


# SECTION: CLASSES ========================================================== #


class ReadinessReportBuilder:
    """Shared builder for ETLPlus runtime readiness reports."""

    # -- Static Methods -- #

    @staticmethod
    def coerce_connector_storage_scheme(
        value: str,
    ) -> str | None:
        """
        Return one normalized storage scheme from raw connector-type text.

        This supports coercing storage schemes embedded in connector type
        fields as a common user error, e.g. "s3" instead of "file" with an S3
        URI path.

        Parameters
        ----------
        value : str
            Raw connector-type text to be coerced.

        Returns
        -------
        str | None
            Normalized storage scheme or None if coercion fails.

        """
        if not value:
            return None
        try:
            return str(StorageScheme.coerce(value))
        except ValueError:
            return None

    @staticmethod
    def coerce_storage_scheme(
        path: str,
    ) -> str | None:
        """
        Return one normalized storage scheme for *path* when present.

        Parameters
        ----------
        path : str
            Path to be analyzed for a storage scheme.

        Returns
        -------
        str | None
            Normalized storage scheme or None if not present.

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

    @staticmethod
    def collect_substitution_tokens(
        value: Any,
    ) -> set[str]:
        """
        Return unresolved ``${VAR}`` token names found in nested values.

        Parameters
        ----------
        value : Any
            Nested value to be analyzed for substitution tokens.

        Returns
        -------
        set[str]
            Set of unresolved token names.

        """
        tokens: set[str] = set()

        def _walk(node: Any) -> None:
            match node:
                case str():
                    for match in _TOKEN_PATTERN.findall(node):
                        tokens.add(match)
                case Mapping():
                    for inner in node.values():
                        _walk(inner)
                case list() | tuple() | set() | frozenset() as seq:
                    for inner in seq:
                        _walk(inner)
                case _:
                    return

        _walk(value)
        return tokens

    @staticmethod
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
            API reference name, if available.
        issue : str
            Description of the connector gap issue.

        Returns
        -------
        str | None
            Actionable guidance string or None if no guidance is available.
        """
        match issue:
            case 'missing path':
                return (
                    'Set "path" to a local path or storage URI for this file connector.'
                )
            case 'missing url or api reference':
                return (
                    'Set "url" to a reachable endpoint or "api" to a configured '
                    'top-level API name.'
                )
            case 'missing connection_string':
                return (
                    'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
                )
            case issue_text if issue_text.startswith('unknown api reference: '):
                if api_reference:
                    return (
                        f'Define "{api_reference}" under top-level "apis" or update '
                        'the connector "api" reference.'
                    )
                return 'Define the referenced API under top-level "apis".'
            case _:
                return None

    @staticmethod
    def dedupe_rows(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Return rows with duplicates removed while preserving order.

        Parameters
        ----------
        rows : list[dict[str, Any]]
            List of rows to be deduplicated.

        Returns
        -------
        list[dict[str, Any]]
            List of rows with duplicates removed.
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

    @staticmethod
    def effective_environment(
        cfg: Config,
        env: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """
        Return the merged environment used for config substitution.

        Parameters
        ----------
        cfg : Config
            Configuration object containing profile information.
        env : Mapping[str, str] | None
            External environment variables to merge.

        Returns
        -------
        dict[str, str]
            Merged environment dictionary.
        """
        base_env = dict(getattr(cfg.profile, 'env', {}) or {})
        external_env = dict(env) if env is not None else dict(os.environ)
        return base_env | external_env

    @staticmethod
    def iter_connectors(
        cfg: Config,
    ) -> Iterator[tuple[str, Connector]]:
        """
        Yield source/target connectors tagged with their role.

        Parameters
        ----------
        cfg : Config
            Configuration object containing source and target connectors.

        Yields
        ------
        tuple[str, Connector]
            Tuples of role ('source' or 'target') and connector instance.
        """
        yield from (('source', connector) for connector in cfg.sources)
        yield from (('target', connector) for connector in cfg.targets)

    @staticmethod
    def load_raw_config(
        config_path: str,
    ) -> StrAnyMap:
        """
        Load raw YAML config and require a mapping root.

        Parameters
        ----------
        config_path : str
            Path to the YAML configuration file.

        Returns
        -------
        StrAnyMap
            Parsed configuration mapping.
        """
        raw = File(Path(config_path), FileFormat.YAML).read()
        mapping = maybe_mapping(raw)
        if mapping is None:
            raise TypeError('Pipeline YAML must have a mapping/object root')
        return dict(mapping)

    @staticmethod
    def make_check(
        name: str,
        status: CheckStatus,
        message: str,
        **details: Any,
    ) -> dict[str, Any]:
        """
        Return one readiness check row.

        Parameters
        ----------
        name : str
            Name of the check.
        status : CheckStatus
            Status of the check.
        message : str
            Message describing the check result.
        **details : Any
            Additional details to include in the check row.

        Returns
        -------
        dict[str, Any]
            Dictionary representing the readiness check row.
        """
        payload: dict[str, Any] = {
            'name': name,
            'status': status,
            'message': message,
        }
        payload.update(details)
        return payload

    @staticmethod
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
        detected_format : str | None, optional
            Detected file format that requires the missing dependency.
        detected_scheme : str | None, optional
            Detected storage scheme that requires the missing dependency.
        package : str
            Name of the missing package.
        extra : str | None, optional
            Name of the ETLPlus extra that includes the missing package.

        Returns
        -------
        str
            Actionable guidance string for the missing dependency.
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

    @staticmethod
    def package_available(
        module_name: str,
    ) -> bool:
        """
        Return whether *module_name* is importable without importing it.

        Parameters
        ----------
        module_name : str
            Name of the module to check for availability.

        Returns
        -------
        bool
            ``True`` if the module is importable, ``False`` otherwise.
        """
        try:
            return find_spec(module_name) is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            return False

    # -- Class Methods -- #

    @classmethod
    def aws_env_hint_present(
        cls,
        env: Mapping[str, str],
    ) -> bool:
        """
        Return whether common AWS credential-chain env hints are present.

        Parameters
        ----------
        env : Mapping[str, str]
            Environment mapping to check for AWS credential hints.

        Returns
        -------
        bool
            ``True`` if any AWS credential hints are present, ``False`` if not.
        """
        return any(bool(env.get(name)) for name in _AWS_ENV_HINTS)

    @classmethod
    def azure_authority_has_account_host(
        cls,
        path: str,
    ) -> bool:
        """
        Return whether one Azure storage path authority embeds an account host.

        Parameters
        ----------
        path : str
            Azure storage path to check.

        Returns
        -------
        bool
            ``True`` if the authority embeds an account host, ``False`` if not.
        """
        authority = urlsplit(path).netloc
        _, separator, account_host = authority.partition('@')
        return bool(separator and account_host)

    @classmethod
    def build(
        cls,
        *,
        config_path: str | None = None,
        env: Mapping[str, str] | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """
        Build a runtime readiness report for the current ETLPlus environment.

        Parameters
        ----------
        config_path : str | None, optional
            Optional pipeline configuration file to validate. Default is ``None``.
        env : Mapping[str, str] | None, optional
            Optional environment mapping used instead of :data:`os.environ`.
        strict : bool, optional
            Whether to enable stricter config diagnostics that surface
            malformed entries normally ignored by the tolerant loader.

        Returns
        -------
        dict[str, Any]
            JSON-serializable readiness report.
        """
        checks: list[dict[str, Any]] = [cls.supported_python_check()]

        if config_path:
            try:
                config_kwargs: dict[str, Any] = {'env': env}
                if strict:
                    config_kwargs['strict'] = True
                checks.extend(cls.config_checks(config_path, **config_kwargs))
            except (OSError, TypeError, ValueError) as exc:
                checks.append(
                    cls.make_check(
                        'config-parse',
                        'error',
                        str(exc),
                        path=config_path,
                    ),
                )
        else:
            checks.append(
                cls.make_check(
                    'config-file',
                    'skipped',
                    (
                        'No configuration file provided; only runtime '
                        'checks were performed.'
                    ),
                ),
            )

        return {
            'status': cls.overall_status(checks),
            'etlplus_version': __version__,
            'python_version': cls.python_version(),
            'checks': checks,
        }

    @classmethod
    def config_checks(
        cls,
        config_path: str,
        *,
        env: Mapping[str, str] | None,
        strict: bool = False,
        include_runtime_checks: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Return readiness checks for one pipeline config path.

        Parameters
        ----------
        config_path : str
            Path to the pipeline configuration file.
        env : Mapping[str, str] | None, optional
            Optional environment mapping used instead of :data:`os.environ`.
        strict : bool, optional
            Whether to enable stricter config diagnostics that surface
            malformed entries normally ignored by the tolerant loader.
        include_runtime_checks : bool, optional
            Whether to include runtime readiness checks.

        Returns
        -------
        list[dict[str, Any]]
            List of readiness check results.
        """
        checks: list[dict[str, Any]] = []
        path = Path(config_path)
        if not path.exists():
            return [
                cls.make_check(
                    'config-file',
                    'error',
                    f'Configuration file does not exist: {path}',
                    path=str(path),
                ),
            ]

        checks.append(
            cls.make_check(
                'config-file',
                'ok',
                f'Configuration file exists: {path}',
                path=str(path),
            ),
        )

        raw = cls.load_raw_config(str(path))
        checks.append(
            cls.make_check(
                'config-parse',
                'ok',
                'Configuration YAML parsed successfully.',
            ),
        )

        context = cls.resolve_config_context(raw, env=env)
        if context.unresolved_tokens:
            checks.append(
                cls.make_check(
                    'config-substitution',
                    'error',
                    'Configuration still contains unresolved substitution tokens.',
                    missing_env=context.unresolved_tokens,
                    references=cls.token_reference_rows(context.resolved_raw),
                    unresolved_tokens=context.unresolved_tokens,
                ),
            )
            return checks

        checks.append(
            cls.make_check(
                'config-substitution',
                'ok',
                'Configuration substitutions resolved successfully.',
            ),
        )
        resolved_cfg = cast(Config, context.resolved_cfg)
        if strict:
            strict_issues = cls.strict_config_issue_rows(raw=context.resolved_raw)
            if strict_issues:
                checks.append(
                    cls.make_check(
                        'config-structure',
                        'error',
                        (
                            'Strict config validation found malformed or '
                            'inconsistent configuration entries.'
                        ),
                        issues=strict_issues,
                    ),
                )
                return checks
            checks.append(
                cls.make_check(
                    'config-structure',
                    'ok',
                    (
                        'Strict config validation found no malformed or '
                        'inconsistent configuration entries.'
                    ),
                ),
            )

        if not include_runtime_checks:
            return checks

        checks.extend(cls.connector_readiness_checks(resolved_cfg))
        checks.extend(
            cls.provider_environment_checks(
                cfg=resolved_cfg,
                env=context.effective_env,
            ),
        )
        return checks

    @classmethod
    def connector_gap_rows(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return connector configuration gaps that will block execution.

        Parameters
        ----------
        cfg : Config
            Configuration object containing connectors to be analyzed.

        Returns
        -------
        list[dict[str, Any]]
            List of connector gap rows describing missing required fields or
            unsupported connector types.
        """
        gaps: list[dict[str, Any]] = []
        for role, connector in cls.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            connector_type = str(getattr(connector, 'type', ''))
            coerced_type = cls.connector_type(connector_type)

            if coerced_type is None:
                gaps.append(
                    {
                        'connector': connector_name,
                        'guidance': cls.connector_type_guidance(connector_type),
                        'issue': 'unsupported type',
                        'role': role,
                        'supported_types': list(cls.connector_type_choices()),
                        'type': connector_type,
                    },
                )
                continue

            if coerced_type == DataConnectorType.FILE:
                path = getattr(connector, 'path', None)
                if not path:
                    gaps.append(
                        {
                            'connector': connector_name,
                            'guidance': cls.connector_gap_guidance(
                                issue='missing path',
                            ),
                            'issue': 'missing path',
                            'role': role,
                            'type': connector_type,
                        },
                    )
            elif coerced_type == DataConnectorType.API:
                url = getattr(connector, 'url', None)
                api_ref = getattr(connector, 'api', None)
                if not url and not api_ref:
                    gaps.append(
                        {
                            'connector': connector_name,
                            'guidance': cls.connector_gap_guidance(
                                issue='missing url or api reference',
                            ),
                            'issue': 'missing url or api reference',
                            'role': role,
                            'type': connector_type,
                        },
                    )
                elif api_ref and api_ref not in cfg.apis:
                    gaps.append(
                        {
                            'connector': connector_name,
                            'guidance': cls.connector_gap_guidance(
                                api_reference=cast(str, api_ref),
                                issue=f'unknown api reference: {api_ref}',
                            ),
                            'issue': f'unknown api reference: {api_ref}',
                            'role': role,
                            'type': connector_type,
                        },
                    )
            elif coerced_type == DataConnectorType.DATABASE:
                connection_string = getattr(connector, 'connection_string', None)
                if not connection_string:
                    gaps.append(
                        {
                            'connector': connector_name,
                            'guidance': cls.connector_gap_guidance(
                                issue='missing connection_string',
                            ),
                            'issue': 'missing connection_string',
                            'role': role,
                            'type': connector_type,
                        },
                    )

        return gaps

    @classmethod
    def connector_readiness_checks(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return connector configuration and dependency readiness checks.

        Parameters
        ----------
        cfg : Config
            Configuration object containing connectors to be analyzed.

        Returns
        -------
        list[dict[str, Any]]
            List of readiness check rows describing connector configuration gaps
            and missing optional dependencies.
        """
        checks: list[dict[str, Any]] = []

        gaps = cls.connector_gap_rows(cfg)
        if gaps:
            checks.append(
                cls.make_check(
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
                cls.make_check(
                    'connector-readiness',
                    'ok',
                    'Configured connectors include the required runtime fields.',
                ),
            )

        missing_requirements = cls.missing_requirement_rows(cfg=cfg)
        if missing_requirements:
            checks.append(
                cls.make_check(
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
                cls.make_check(
                    'optional-dependencies',
                    'ok',
                    (
                        'No missing optional dependencies were detected for '
                        'configured connectors.'
                    ),
                ),
            )

        return checks

    @classmethod
    def connector_type(
        cls,
        connector_type: str,
    ) -> DataConnectorType | None:
        """
        Return one coerced connector type or ``None`` when unsupported.

        Parameters
        ----------
        connector_type : str
            Raw connector type text to be coerced.

        Returns
        -------
        DataConnectorType | None
            Coerced connector type or ``None`` if unsupported.
        """
        try:
            return DataConnectorType.coerce(connector_type)
        except ValueError:
            return None

    @classmethod
    def connector_type_choices(cls) -> tuple[str, ...]:
        """
        Return the supported connector type names.

        Returns
        -------
        tuple[str, ...]
            Supported connector type names.
        """
        return tuple(str(member.value) for member in DataConnectorType)

    @classmethod
    def connector_type_guidance(
        cls,
        connector_type: str,
    ) -> str:
        """
        Return actionable guidance for an unsupported connector type.

        Parameters
        ----------
        connector_type : str
            Raw connector type text that was found to be unsupported.

        Returns
        -------
        str
            Actionable guidance for the unsupported connector type.
        """
        supported = ', '.join(cls.connector_type_choices())
        normalized = connector_type.strip().lower()
        if not normalized:
            return f'Set type to one of: {supported}.'

        storage_scheme = cls.coerce_connector_storage_scheme(normalized)
        if storage_scheme is not None:
            return (
                f'"{normalized}" is a storage scheme, not a connector type. '
                'Use connector type "file" and keep the provider in the path '
                'or URI scheme.'
            )

        return f'Use one of the supported connector types: {supported}.'

    @classmethod
    def explicit_aws_credential_gap(
        cls,
        env: Mapping[str, str],
    ) -> dict[str, Any] | None:
        """
        Return one AWS env error row for incomplete explicit credentials.

        This checks for the common user error of setting only part of the
        explicit AWS credential env vars (e.g. only the access key without the
        secret key), which would lead to runtime errors when accessing S3
        paths. This is only surfaced as an error if at least one of the
        explicit credential env vars is set, to avoid false positives for users
        relying on other credential resolution methods like AWS profiles,
        shared config files, container credentials, or instance metadata. This
        check is only relevant for S3 paths, so it should be called
        conditionally when S3 storage schemes are detected in the config. AWS
        credential env vars (e.g. only the access key without the secret key),
        which would lead to runtime errors when accessing S3 paths. This is only
        surfaced as an error if at least one of the explicit credential env
        vars is set, to avoid false positives for users relying on other
        credential resolution methods like AWS profiles, shared config files,
        container credentials, or instance metadata. This check is only
        relevant for S3 paths, so it should be called conditionally when S3
        storage schemes are detected in the config.

        Parameters
        ----------
        env : Mapping[str, str]
            Environment mapping to check for AWS credential env vars.
        Returns
        -------
        dict[str, Any] | None
            AWS credential env error row if incomplete explicit credentials are
            detected, or ``None`` if no issues are found.
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

    @classmethod
    def missing_requirement_rows(
        cls,
        *,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return missing optional dependency rows for configured connectors.

        Parameters
        ----------
        cfg : Config
            Configuration object containing connectors to be analyzed.
        Returns
        -------
        list[dict[str, Any]]
            List of missing optional dependency rows describing which
            dependencies are required by the config and not currently available
            in the environment.
        """
        rows: list[dict[str, Any]] = []

        for role, connector in cls.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            path = getattr(connector, 'path', None)
            format_name = str(getattr(connector, 'format', '') or '').lower()

            if path:
                scheme = cls.coerce_storage_scheme(path)
                if scheme and (requirement := _SCHEME_EXTRA_REQUIREMENTS.get(scheme)):
                    if not cls.requirement_available(requirement):
                        rows.append(
                            cls.requirement_row(
                                connector=connector_name,
                                detected_scheme=scheme,
                                reason=(
                                    f'{scheme} storage path requires '
                                    f'{requirement.package}'
                                ),
                                requirement=requirement,
                                role=role,
                            ),
                        )

            if format_name == 'nc':
                if not cls.netcdf_available():
                    rows.append(
                        {
                            'connector': connector_name,
                            'detected_format': 'nc',
                            'extra': 'file',
                            'guidance': cls.missing_requirement_guidance(
                                detected_format='nc',
                                package='xarray/netCDF4',
                                extra='file',
                            ),
                            'missing_package': 'xarray/netCDF4',
                            'reason': (
                                'nc format requires xarray and netCDF4 or h5netcdf'
                            ),
                            'role': role,
                        },
                    )
                continue

            if requirement := _FORMAT_EXTRA_REQUIREMENTS.get(format_name):
                if not cls.requirement_available(requirement):
                    rows.append(
                        cls.requirement_row(
                            connector=connector_name,
                            detected_format=format_name,
                            reason=(
                                f'{format_name} format requires {requirement.package}'
                            ),
                            requirement=requirement,
                            role=role,
                        ),
                    )

        return cls.dedupe_rows(rows)

    @classmethod
    def provider_environment_checks(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """
        Return provider-specific environment readiness checks.

        Parameters
        ----------
        cfg : Config
            Configuration object containing connectors to be analyzed for
            provider-specific environment gaps.
        env : Mapping[str, str]
            Environment mapping to check for provider-specific environment
            gaps.

        Returns
        -------
        list[dict[str, Any]]
            List of readiness check rows describing provider-specific
            environment gaps and their severity based on the potential impact
            on runtime execution.
        """
        rows = cls.provider_environment_rows(cfg=cfg, env=env)
        if not rows:
            return [
                cls.make_check(
                    'provider-environment',
                    'ok',
                    'No provider-specific environment gaps were detected.',
                ),
            ]

        has_error = any(row['severity'] == 'error' for row in rows)
        errors = sum(1 for row in rows if row['severity'] == 'error')
        warnings = sum(1 for row in rows if row['severity'] == 'warn')
        return [
            cls.make_check(
                'provider-environment',
                'error' if has_error else 'warn',
                (
                    f'Provider environment gaps: {errors} error(s), '
                    f'{warnings} warning(s).'
                    if has_error
                    else f'Provider environment warnings: {warnings}.'
                ),
                environment_gaps=rows,
            ),
        ]

    @classmethod
    def provider_environment_rows(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """
        Return provider-specific environment gaps for configured connectors.

        Parameters
        ----------
        cfg : Config
            Configuration object containing connectors to be analyzed for
            provider-specific environment gaps.
        env : Mapping[str, str]
            Environment mapping to check for provider-specific environment gaps.

        Returns
        -------
        list[dict[str, Any]]
            List of readiness check rows describing provider-specific environment
            gaps and their severity based on the potential impact on runtime execution.

        """
        rows: list[dict[str, Any]] = []

        azure_connection_string = bool(env.get('AZURE_STORAGE_CONNECTION_STRING'))
        azure_account_url = bool(env.get('AZURE_STORAGE_ACCOUNT_URL'))
        azure_credential = bool(env.get(_AZURE_STORAGE_CREDENTIAL_ENV))
        aws_env_hint_present = cls.aws_env_hint_present(env)

        for role, connector in cls.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            path = getattr(connector, 'path', None)
            if not isinstance(path, str) or not path:
                continue

            scheme = cls.coerce_storage_scheme(path)
            match scheme:
                case 'azure-blob' | 'abfs':
                    provider = 'azure-storage'
                    authority_has_account_host = cls.azure_authority_has_account_host(
                        path,
                    )
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
                                'provider': provider,
                                'reason': (
                                    f'{scheme} path does not provide an account '
                                    'host and no Azure storage bootstrap '
                                    'settings were found.'
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
                                    'Set AZURE_STORAGE_CREDENTIAL when the '
                                    'target is not public, or use '
                                    'AZURE_STORAGE_CONNECTION_STRING for a '
                                    'fully explicit configuration.'
                                ),
                                'missing_env': [_AZURE_STORAGE_CREDENTIAL_ENV],
                                'provider': provider,
                                'reason': (
                                    f'{scheme} access has no explicit Azure '
                                    'credential configured; runtime access will '
                                    'only work for public resources or other '
                                    'ambient authentication handled by the SDK '
                                    'call site.'
                                ),
                                'role': role,
                                'scheme': scheme,
                                'severity': 'warn',
                            },
                        )
                case 's3':
                    if explicit_gap := cls.explicit_aws_credential_gap(env):
                        rows.append(
                            {
                                'connector': connector_name,
                                'role': role,
                                'scheme': scheme,
                            }
                            | explicit_gap,
                        )
                        continue
                    if not aws_env_hint_present:
                        rows.append(
                            {
                                'connector': connector_name,
                                'guidance': (
                                    'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                                    'AWS_SECRET_ACCESS_KEY, or rely on shared '
                                    'config files, container credentials, or '
                                    'instance metadata.'
                                ),
                                'missing_env': list(_AWS_ENV_HINTS),
                                'provider': 'aws-s3',
                                'reason': (
                                    'No common AWS credential-chain '
                                    'environment hints were detected for this '
                                    'S3 path.'
                                ),
                                'role': role,
                                'scheme': scheme,
                                'severity': 'warn',
                            },
                        )

        return rows

    @classmethod
    def netcdf_available(cls) -> bool:
        """
        Return whether netCDF support dependencies are installed.

        Returns
        -------
        bool
            ``True`` if netCDF support dependencies are installed, ``False``
            if not.
        """
        return cls.package_available('xarray') and (
            cls.package_available('netCDF4') or cls.package_available('h5netcdf')
        )

    @classmethod
    def overall_status(
        cls,
        checks: list[dict[str, Any]],
    ) -> Literal['ok', 'warn', 'error']:
        """
        Return aggregate status from individual check rows.

        Parameters
        ----------
        checks : list[dict[str, Any]]
            List of individual check result rows to aggregate into an overall
            status.

        Returns
        -------
        Literal['ok', 'warn', 'error']
            Aggregate status based on the individual check rows.
        """
        statuses = {cast(CheckStatus, check['status']) for check in checks}
        if 'error' in statuses:
            return 'error'
        if 'warn' in statuses:
            return 'warn'
        return 'ok'

    @classmethod
    def python_version(cls) -> str:
        """
        Return the current interpreter version as dotted text.

        Returns
        -------
        str
            Current interpreter version in "major.minor.micro" format.
        """
        return (
            f'{sys.version_info.major}.'
            f'{sys.version_info.minor}.'
            f'{sys.version_info.micro}'
        )

    @classmethod
    def requirement_available(
        cls,
        requirement: _RequirementSpec,
    ) -> bool:
        """
        Return whether any module for one requirement is importable.

        Parameters
        ----------
        requirement : _RequirementSpec
            Requirement specification to check.

        Returns
        -------
        bool
            ``True`` if any module for the requirement is importable, ``False``
            if not.
        """
        return any(
            cls.package_available(module_name) for module_name in requirement.modules
        )

    @classmethod
    def requirement_row(
        cls,
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
            Name of the connector with the missing requirement.
        detected_format : str | None, optional
            Optional detected format that triggers the requirement. Default is
            ``None``.
        detected_scheme : str | None, optional
            Optional detected storage scheme that triggers the requirement.
            Default is ``None``.
        reason : str
            Explanation of why the requirement is needed based on the config
            analysis.
        requirement : _RequirementSpec
            The missing requirement specification.
        role : str
            The role of the connector (e.g. "source", "target") with the
            missing requirement.

        Returns
        -------
        dict[str, Any]
            A dictionary representing the missing requirement row.
        """
        row: dict[str, Any] = {
            'connector': connector,
            'extra': requirement.extra or '',
            'guidance': cls.missing_requirement_guidance(
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

    @classmethod
    def resolve_config_context(
        cls,
        raw: StrAnyMap,
        *,
        env: Mapping[str, str] | None,
    ) -> _ResolvedConfigContext:
        """
        Return resolved config state shared by config readiness checks.

        This performs deep substitution on the raw config and collects any
        remaining unresolved substitution tokens for use in config checks that
        need to report on unresolved tokens or references. This avoids redundant
        substitution and token collection in individual checks that need this
        information, and ensures consistent reporting of unresolved tokens
        across checks.

        Parameters
        ----------
        raw : StrAnyMap
            Raw configuration mapping loaded from YAML, before any substitution.
        env : Mapping[str, str] | None
            Optional environment mapping used for substitution, falling back to
            :data:`os.environ` when not provided.

        Returns
        -------
        _ResolvedConfigContext
            Resolved config context containing the raw config, effective
            environment, resolved config with substitutions applied, and any
            unresolved substitution tokens.
        """
        cfg = Config.from_dict(raw)
        effective_env = cls.effective_environment(cfg, env)
        resolved = cast(StrAnyMap, deep_substitute(raw, cfg.vars, effective_env))
        unresolved_tokens = sorted(cls.collect_substitution_tokens(resolved))
        return _ResolvedConfigContext(
            raw=raw,
            effective_env=effective_env,
            unresolved_tokens=unresolved_tokens,
            resolved_raw=resolved,
            resolved_cfg=(None if unresolved_tokens else Config.from_dict(resolved)),
        )

    @classmethod
    def strict_config_issue_rows(
        cls,
        *,
        raw: StrAnyMap,
    ) -> list[dict[str, Any]]:
        """
        Return strict-mode config issues hidden by tolerant parsing.

        Parameters
        ----------
        raw : StrAnyMap
            Raw configuration mapping loaded from YAML, before any
            substitution.

        Returns
        -------
        list[dict[str, Any]]
            List of strict-mode config issues describing malformed or
            inconsistent configuration entries that would be hidden by tolerant
            parsing and could lead to runtime errors or unexpected behavior.
        """
        issues: list[dict[str, Any]] = []
        source_names = cls.strict_connector_names(
            raw=raw,
            section='sources',
            issues=issues,
        )
        target_names = cls.strict_connector_names(
            raw=raw,
            section='targets',
            issues=issues,
        )
        transform_names = cls.strict_named_section_names(
            raw=raw,
            section='transforms',
            issues=issues,
            guidance='Define transforms as a mapping keyed by pipeline name.',
        )
        validation_names = cls.strict_named_section_names(
            raw=raw,
            section='validations',
            issues=issues,
            guidance='Define validations as a mapping keyed by ruleset name.',
        )
        cls.strict_job_issue_rows(
            raw=raw,
            issues=issues,
            source_names=source_names,
            target_names=target_names,
            transform_names=transform_names,
            validation_names=validation_names,
        )
        return issues

    @classmethod
    def strict_config_report(
        cls,
        *,
        config_path: str,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Return one strict config-validation report for the CLI command
        ``etlplus check --strict``.
        This runs the strict config validation checks and returns a report
        containing the overall status, the ETL+ version, and the individual check
        results with any strict-mode config issues found. This is used for the
        ``etlplus check --strict`` CLI command to provide users with detailed
        feedback on any config issues that are hidden by tolerant parsing and
        could lead to runtime errors or unexpected behavior, along with
        actionable guidance for fixing the issues.

        Parameters
        ----------
        config_path : str
            Path to the config file being checked, used for context in error
            messages.
        env : Mapping[str, str] | None, optional
            Optional environment mapping to use for substitution and
            environment checks, falling back to :data:`os.environ` when not
            provided.

        Returns
        -------
        dict[str, Any]
            Strict config-validation report containing the overall status,
            ETL+ version, and individual check results with any strict-mode
            config issues found.
        """
        checks = cls.config_checks(
            config_path,
            env=env,
            strict=True,
            include_runtime_checks=False,
        )
        return {
            'status': cls.overall_status(checks),
            'etlplus_version': __version__,
            'checks': checks,
        }

    @classmethod
    def strict_connector_names(
        cls,
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
    ) -> set[str] | None:
        """
        Validate connector entries in *section* and return known names.

        This checks that the connectors in the specified section are defined as
        a list of mappings with valid types and non-empty names, and that there
        are no duplicate names within the section. This is a strict-mode check
        that surfaces issues that would be hidden by tolerant parsing, such as
        connectors defined as a single mapping instead of a list, connectors
        with missing or invalid types, connectors with blank names, and
        duplicate connector names within the section. Any issues found are
        appended to *issues* with actionable guidance for fixing the issues. If
        no issues are found, the set of connector names in the section is
        returned for use in validating job references. If issues are found that
        prevent reliable extraction of connector names, ``None`` is returned to
        indicate that job reference validation should be skipped for this
        section.

        Parameters
        ----------
        raw : StrAnyMap
            Raw configuration mapping loaded from YAML, before any
            substitution.
        section : str
            The connector section to validate (e.g. "sources", "targets").
        issues : list[dict[str, Any]]
            List to append any strict-mode config issues found during
            validation, with actionable guidance for fixing the issues.

        Returns
        -------
        set[str] | None
            Set of connector names in the section if no issues are found, or
            ``None`` if issues are found that prevent reliable extraction of
            connector names.
        """
        value = raw.get(section)
        if value is None:
            return set()
        if not isinstance(value, list):
            issues.append(
                {
                    'expected': 'list',
                    'guidance': (
                        f'Define {section} as a YAML list of connector mappings.'
                    ),
                    'issue': 'invalid section type',
                    'observed_type': type(value).__name__,
                    'section': section,
                },
            )
            return None

        names: set[str] = set()
        seen: set[str] = set()
        for index, entry in enumerate(value):
            if not isinstance(entry, Mapping):
                issues.append(
                    {
                        'guidance': (
                            'Define each connector as a mapping with at least '
                            '"name" and "type" fields.'
                        ),
                        'index': index,
                        'issue': 'invalid connector entry',
                        'observed_type': type(entry).__name__,
                        'section': section,
                    },
                )
                continue
            try:
                connector = parse_connector(entry)
            except TypeError as exc:
                raw_type = entry.get('type')
                guidance = None
                if isinstance(raw_type, str):
                    guidance = cls.connector_type_guidance(raw_type)
                elif raw_type is None:
                    guidance = (
                        'Set "type" to one of: '
                        + ', '.join(cls.connector_type_choices())
                        + '.'
                    )
                issues.append(
                    {
                        'guidance': guidance,
                        'index': index,
                        'issue': 'invalid connector entry',
                        'message': str(exc),
                        'section': section,
                    },
                )
                continue

            name = str(getattr(connector, 'name', '') or '').strip()
            if not name:
                issues.append(
                    {
                        'guidance': 'Set "name" to a non-empty string.',
                        'index': index,
                        'issue': 'blank connector name',
                        'section': section,
                    },
                )
                continue
            if name in seen:
                issues.append(
                    {
                        'guidance': f'Use unique connector names within {section}.',
                        'index': index,
                        'issue': f'duplicate connector name: {name}',
                        'section': section,
                    },
                )
            seen.add(name)
            names.add(name)

        return names

    @classmethod
    def strict_job_issue_rows(
        cls,
        *,
        raw: StrAnyMap,
        issues: list[dict[str, Any]],
        source_names: set[str] | None,
        target_names: set[str] | None,
        transform_names: set[str] | None,
        validation_names: set[str] | None,
    ) -> None:
        """
        Append strict-mode job diagnostics to *issues*.

        This checks that the jobs are defined as a list of mappings with valid
        types and non-empty names, that there are no duplicate job names, and
        that the extract, load, transform, and validate sections of each job
        have valid references to configured sources, targets, transforms, and
        validations. This is a strict-mode check that surfaces issues that
        would be hidden by tolerant parsing, such as jobs defined as a single
        mapping instead of a list, jobs with missing or invalid types or names,
        duplicate job names, and job sections with missing or invalid
        references to configured resources. Any issues found are appended to
        *issues* with actionable guidance for fixing the issues. The
        *source_names*, *target_names*, *transform_names*, and
        *validation_names* parameters are used to validate job references to
        configured resources, and should be passed as the sets of names
        returned by :meth:`strict_connector_names` and
        :meth:`strict_named_section_names` for the respective sections. If any
        of these name sets are ``None`` due to issues found in the respective
        sections, job reference validation for that section will be skipped to
        avoid unreliable reference validation and overwhelming users with
        cascading issues.

        Parameters
        ----------
        raw : StrAnyMap
            Raw configuration mapping loaded from YAML, before any substitution.
        issues : list[dict[str, Any]]
            List to append any strict-mode config issues found during
            validation, with actionable guidance for fixing the issues.
        source_names : set[str] | None
            Set of valid source names for validating job extract references, or
            ``None`` if issues were found in the sources section that prevent
            reliable extraction of source names.
        target_names : set[str] | None
            Set of valid target names for validating job load references, or
            ``None`` if issues were found in the targets section that prevent
            reliable extraction of target names.
        transform_names : set[str] | None
            Set of valid transform pipeline names for validating job transform
            references, or ``None`` if issues were found in the transforms
            section that prevent reliable extraction of transform names.
        validation_names : set[str] | None
            Set of valid validation ruleset names for validating job validate
            references, or ``None`` if issues were found in the validations
            section that prevent reliable extraction of validation names.
       """
        value = raw.get('jobs')
        if value is None:
            return
        if not isinstance(value, list):
            issues.append(
                {
                    'expected': 'list',
                    'guidance': 'Define jobs as a YAML list of job mappings.',
                    'issue': 'invalid section type',
                    'observed_type': type(value).__name__,
                    'section': 'jobs',
                },
            )
            return

        seen_jobs: set[str] = set()
        for index, entry in enumerate(value):
            if not isinstance(entry, Mapping):
                issues.append(
                    {
                        'guidance': (
                            'Define each job as a mapping with "name", '
                            '"extract", and "load" sections.'
                        ),
                        'index': index,
                        'issue': 'invalid job entry',
                        'observed_type': type(entry).__name__,
                        'section': 'jobs',
                    },
                )
                continue

            raw_name = entry.get('name')
            job_name = raw_name.strip() if isinstance(raw_name, str) else None
            if not job_name:
                issues.append(
                    {
                        'guidance': 'Set "name" to a non-empty string.',
                        'index': index,
                        'issue': 'missing job name',
                        'section': 'jobs',
                    },
                )
            elif job_name in seen_jobs:
                issues.append(
                    {
                        'guidance': 'Use unique job names within jobs.',
                        'index': index,
                        'issue': f'duplicate job name: {job_name}',
                        'job': job_name,
                        'section': 'jobs',
                    },
                )
            else:
                seen_jobs.add(job_name)

            cls.strict_job_ref_issue(
                entry=entry,
                field='extract',
                index=index,
                issues=issues,
                job_name=job_name,
                required=True,
                required_key='source',
                section_names=source_names,
                section_label='sources',
            )
            cls.strict_job_ref_issue(
                entry=entry,
                field='load',
                index=index,
                issues=issues,
                job_name=job_name,
                required=True,
                required_key='target',
                section_names=target_names,
                section_label='targets',
            )
            cls.strict_job_ref_issue(
                entry=entry,
                field='transform',
                index=index,
                issues=issues,
                job_name=job_name,
                required=False,
                required_key='pipeline',
                section_names=transform_names,
                section_label='transforms',
            )
            cls.strict_job_ref_issue(
                entry=entry,
                field='validate',
                index=index,
                issues=issues,
                job_name=job_name,
                required=False,
                required_key='ruleset',
                section_names=validation_names,
                section_label='validations',
            )

    @classmethod
    def strict_job_ref_issue(
        cls,
        *,
        entry: Mapping[str, Any],
        field: str,
        index: int,
        issues: list[dict[str, Any]],
        job_name: str | None,
        required: bool,
        required_key: str,
        section_names: set[str] | None,
        section_label: str,
    ) -> None:
        """
        Append one strict-mode job reference issue when needed.
        This checks one job section (extract, load, transform, or validate) for
        missing or invalid references to configured resources, and appends an
        issue to *issues* with actionable guidance if any issues are found.
        This is a strict-mode check that surfaces issues that would be hidden
        by tolerant parsing, such as missing sections, sections with invalid
        types, missing required keys, blank references, and references to
        unknown resources. The *section_names* and *section_label* parameters
        are used to validate references to configured resources in the
        respective sections, and should be passed as the sets of names returned
        by :meth:`strict_connector_names` and
        :meth:`strict_named_section_names` for the respective sections. If
        *section_names* is ``None`` due to issues found in the respective
        section, reference validation for that section will be skipped to avoid
        unreliable reference validation and overwhelming users with cascading
        issues. The *job_name* parameter is used to provide context in issue
        messages when available, and the *required* parameter indicates whether
        the section is required (e.g. extract and load) or optional (e.g.
        transform and validate) for the job, which is used to tailor issue
        messages and guidance accordingly.

        Parameters
        ----------
        entry : Mapping[str, Any]
            The job entry mapping to check the section in.
        field : str
            The job section to check (e.g., "extract", "load", "transform",
            "validate").
        index : int
            The index of the job entry in the jobs list, used for context in
            issue messages.
        issues : list[dict[str, Any]]
            List to append any strict-mode config issues found during
            validation, with actionable guidance for fixing the issues.
        job_name : str | None
            The name of the job being checked, used for context in issue
            messages when available.
        required : bool
            Whether the section being checked is required for the job, which is
            used to tailor issue messages and guidance accordingly.
        required_key : str
            The required key that should be present in the section mapping
            (e.g. "source" for extract, "target" for load, "pipeline" for
            transform, "ruleset" for validate), used for validating the section
            structure and providing specific guidance for fixing issues.
        section_names : set[str] | None
            Set of valid names for the referenced resources in the section
            (e.g. source names for extract, target names for load, transform
            pipeline names for transform, validation ruleset names for
            validate), used for validating that references point to configured
            resources. If ``None``, reference validation will be skipped for
            this section to avoid unreliable validation and overwhelming users
            with cascading issues when there are issues in the respective
            section that prevent reliable extraction of valid names.
        section_label : str
            Human-friendly label for the section being referenced (e.g.,
            "sources" for extract, "targets" for load, "transforms" for
            transform, "validations" for validate), used for tailoring issue
            messages and guidance.
        """
        value = entry.get(field)
        base_issue: dict[str, Any] = {
            'field': (
                field if field in {'extract', 'load'} else f'{field}.{required_key}'
            ),
            'index': index,
            'section': 'jobs',
        }
        if job_name:
            base_issue['job'] = job_name

        if value is None:
            if required:
                issues.append(
                    base_issue
                    | {
                        'guidance': (
                            f'Add a {field} mapping with "{required_key}" '
                            'set to a configured resource name.'
                        ),
                        'issue': f'missing {field} section',
                    },
                )
            return

        if not isinstance(value, Mapping):
            issues.append(
                base_issue
                | {
                    'guidance': (
                        f'Define {field} as a mapping with a '
                        f'"{required_key}" string field.'
                    ),
                    'issue': f'invalid {field} section',
                    'observed_type': type(value).__name__,
                },
            )
            return

        ref_value = value.get(required_key)
        ref_name = ref_value.strip() if isinstance(ref_value, str) else None
        if not ref_name:
            issues.append(
                base_issue
                | {
                    'guidance': (
                        f'Set {field}.{required_key} to a configured resource name.'
                    ),
                    'issue': f'missing {field}.{required_key}',
                },
            )
            return

        if section_names is not None and ref_name not in section_names:
            issues.append(
                base_issue
                | {
                    'guidance': (
                        f'Define "{ref_name}" under top-level "{section_label}" '
                        f'or update {field}.{required_key}.'
                    ),
                    'issue': (f'unknown {section_label[:-1]} reference: {ref_name}'),
                },
            )

    @classmethod
    def strict_named_section_names(
        cls,
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
        guidance: str,
    ) -> set[str] | None:
        """
        Validate one mapping-like top-level section and return its keys.

        This checks that the specified section is defined as a mapping with
        valid types and non-empty keys, and that there are no duplicate keys
        within the section. This is a strict-mode check that surfaces issues
        that would be hidden by tolerant parsing, such as sections defined as a
        list instead of a mapping, sections with invalid types, and sections
        with blank keys. Any issues found are appended to *issues* with
        actionable guidance for fixing the issues. If no issues are found, the
        set of keys in the section is returned for use in validating job
        references. If issues are found that prevent reliable extraction of
        keys, ``None`` is returned to indicate that job reference validation
        should be skipped for this section.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        section : str
            The name of the section to validate.
        issues : list[dict[str, Any]]
            A list to which any issues found will be appended.
        guidance : str
            Guidance message for fixing issues.

        Returns
        -------
        set[str] | None
            A set of keys in the section if no issues are found, ``None`` if
            issues are found that prevent reliable extraction of keys.
        """
        value = raw.get(section)
        if value is None:
            return set()
        if not isinstance(value, Mapping):
            issues.append(
                {
                    'expected': 'mapping',
                    'guidance': guidance,
                    'issue': 'invalid section type',
                    'observed_type': type(value).__name__,
                    'section': section,
                },
            )
            return None
        return {str(name) for name in value}

    @classmethod
    def supported_python_check(
        cls,
    ) -> dict[str, Any]:
        """
        Return runtime Python compatibility check.

        Returns
        -------
        dict[str, Any]
            A dictionary containing the Python compatibility check results.
        """
        version = cls.python_version()
        minimum, maximum = _SUPPORTED_PYTHON_RANGE
        supported = minimum <= sys.version_info[:2] < maximum
        if supported:
            return cls.make_check(
                'python-version',
                'ok',
                f'Python {version} is within the supported ETLPlus runtime range.',
                version=version,
            )
        return cls.make_check(
            'python-version',
            'error',
            (
                f'Python {version} is outside the supported ETLPlus runtime '
                f'range (>={minimum[0]}.{minimum[1]},'
                f'<{maximum[0]}.{maximum[1]}).'
            ),
            version=version,
        )

    @classmethod
    def token_reference_rows(
        cls,
        value: Any,
    ) -> list[dict[str, Any]]:
        """
        Return unresolved token rows with stable dotted/indexed paths.

        This recursively walks the given value and collects any strings
        containing unresolved substitution tokens, returning a list of rows
        with the token names and their corresponding paths in the config. The
        paths are represented in a stable dotted/indexed format that can be
        used in check reports to provide clear context for where unresolved
        tokens are located in the config, even when the config structure is
        complex and nested. This is used in checks that need to report on
        unresolved tokens or references in the config, allowing them to provide
        actionable feedback to users on where unresolved tokens are located and
        what they are, which can help users identify and fix issues with their
        config more effectively.

        Parameters
        ----------
        value : Any
            The value to recursively walk for collecting unresolved
            substitution tokens.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries, each containing a token name and its
            corresponding paths in the config where it is found.
        """
        paths_by_name: dict[str, set[str]] = {}

        def _walk(node: Any, path: str = '') -> None:
            match node:
                case str():
                    for match in _TOKEN_PATTERN.findall(node):
                        paths_by_name.setdefault(match, set()).add(path or '<root>')
                case Mapping():
                    for key, inner in node.items():
                        key_text = str(key)
                        next_path = f'{path}.{key_text}' if path else key_text
                        _walk(inner, next_path)
                case list() | tuple() as seq:
                    for index, inner in enumerate(seq):
                        next_path = f'{path}[{index}]' if path else f'[{index}]'
                        _walk(inner, next_path)
                case set() | frozenset():
                    for index, inner in enumerate(sorted(node, key=repr)):
                        next_path = f'{path}[{index}]' if path else f'[{index}]'
                        _walk(inner, next_path)
                case _:
                    return

        _walk(value)
        return [
            {
                'name': name,
                'paths': sorted(paths),
            }
            for name, paths in sorted(paths_by_name.items())
        ]
