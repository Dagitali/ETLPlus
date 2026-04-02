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

    raw: StrAnyMap
    effective_env: dict[str, str]
    unresolved_tokens: list[str]
    resolved_raw: StrAnyMap
    resolved_cfg: Config | None


# SECTION: INTERNAL CONSTANTS =============================================== #

_AWS_ENV_HINTS: Final[tuple[str, ...]] = (
    'AWS_ACCESS_KEY_ID',
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
        """Return one normalized storage scheme from raw connector-type text."""
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
        """Return one normalized storage scheme for *path* when present."""
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
        """Return unresolved ``${VAR}`` token names found in nested values."""
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
    def dedupe_rows(
        rows: list[dict[str, str]],
    ) -> list[dict[str, str]]:
        """Return rows with duplicates removed while preserving order."""
        unique_rows: list[dict[str, str]] = []
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
        """Return the merged environment used for config substitution."""
        base_env = dict(getattr(cfg.profile, 'env', {}) or {})
        external_env = dict(env) if env is not None else dict(os.environ)
        return base_env | external_env

    @staticmethod
    def iter_connectors(
        cfg: Config,
    ) -> Iterator[tuple[str, Connector]]:
        """Yield source/target connectors tagged with their role."""
        yield from (('source', connector) for connector in cfg.sources)
        yield from (('target', connector) for connector in cfg.targets)

    @staticmethod
    def load_raw_config(
        config_path: str,
    ) -> StrAnyMap:
        """Load raw YAML config and require a mapping root."""
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
        """Return one readiness check row."""
        payload: dict[str, Any] = {
            'name': name,
            'status': status,
            'message': message,
        }
        payload.update(details)
        return payload

    @staticmethod
    def package_available(
        module_name: str,
    ) -> bool:
        """Return whether *module_name* is importable without importing it."""
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
        """Return whether common AWS credential-chain env hints are present."""
        return any(bool(env.get(name)) for name in _AWS_ENV_HINTS)

    @classmethod
    def azure_authority_has_account_host(
        cls,
        path: str,
    ) -> bool:
        """Return whether one Azure storage path authority embeds an account host."""
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
        """Return readiness checks for one pipeline config path."""
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
        """Return connector configuration gaps that will block execution."""
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
                            'issue': 'missing url or api reference',
                            'role': role,
                            'type': connector_type,
                        },
                    )
                elif api_ref and api_ref not in cfg.apis:
                    gaps.append(
                        {
                            'connector': connector_name,
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
        """Return connector configuration and dependency readiness checks."""
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
        """Return one coerced connector type or ``None`` when unsupported."""
        try:
            return DataConnectorType.coerce(connector_type)
        except ValueError:
            return None

    @classmethod
    def connector_type_choices(cls) -> tuple[str, ...]:
        """Return the supported connector type names."""
        return tuple(str(member.value) for member in DataConnectorType)

    @classmethod
    def connector_type_guidance(
        cls,
        connector_type: str,
    ) -> str:
        """Return actionable guidance for an unsupported connector type."""
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
    def missing_requirement_rows(
        cls,
        *,
        cfg: Config,
    ) -> list[dict[str, str]]:
        """Return missing optional dependency rows for configured connectors."""
        rows: list[dict[str, str]] = []

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
                            'extra': 'file',
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
        """Return provider-specific environment readiness checks."""
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
        """Return provider-specific environment gaps for configured connectors."""
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
                                'severity': 'warn',
                            },
                        )
                case 's3':
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
                                'severity': 'warn',
                            },
                        )

        return rows

    @classmethod
    def netcdf_available(cls) -> bool:
        """Return whether netCDF support dependencies are installed."""
        return cls.package_available('xarray') and (
            cls.package_available('netCDF4') or cls.package_available('h5netcdf')
        )

    @classmethod
    def overall_status(
        cls,
        checks: list[dict[str, Any]],
    ) -> Literal['ok', 'warn', 'error']:
        """Return aggregate status from individual check rows."""
        statuses = {cast(CheckStatus, check['status']) for check in checks}
        if 'error' in statuses:
            return 'error'
        if 'warn' in statuses:
            return 'warn'
        return 'ok'

    @classmethod
    def python_version(cls) -> str:
        """Return the current interpreter version as dotted text."""
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
        """Return whether any module for one requirement is importable."""
        return any(
            cls.package_available(module_name) for module_name in requirement.modules
        )

    @classmethod
    def requirement_row(
        cls,
        *,
        connector: str,
        reason: str,
        requirement: _RequirementSpec,
        role: str,
    ) -> dict[str, str]:
        """Return one missing-requirement row."""
        return {
            'connector': connector,
            'extra': requirement.extra or '',
            'missing_package': requirement.package,
            'reason': reason,
            'role': role,
        }

    @classmethod
    def resolve_config_context(
        cls,
        raw: StrAnyMap,
        *,
        env: Mapping[str, str] | None,
    ) -> _ResolvedConfigContext:
        """Return resolved config state shared by config readiness checks."""
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
        """Return strict-mode config issues hidden by tolerant parsing."""
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
        """Return one strict config-validation report for ``check --strict``."""
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
        """Validate connector entries in *section* and return known names."""
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
        """Append strict-mode job diagnostics to *issues*."""
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
            elif job_name:
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
        """Append one strict-mode job reference issue when needed."""
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
        """Validate one mapping-like top-level section and return its keys."""
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
        """Return runtime Python compatibility check."""
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
