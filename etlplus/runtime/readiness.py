"""
:mod:`etlplus.runtime.readiness` module.

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
from ..config import Config
from ..connector import Connector
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..storage import StorageScheme
from ..utils import deep_substitute
from ..utils import maybe_mapping
from ..utils.types import StrAnyMap

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


# SECTION: INTERNAL CONSTANTS =============================================== #


_SUPPORTED_PYTHON_RANGE: Final[tuple[tuple[int, int], tuple[int, int]]] = (
    (3, 13),
    (3, 15),
)
_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r'\$\{([^}]+)\}')


_FORMAT_EXTRA_REQUIREMENTS: Final[
    dict[str, _RequirementSpec]
] = {
    'dta': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'hdf5': _RequirementSpec(('tables',), 'tables'),
    'rda': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'rds': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'sav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'zsav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
}
_SCHEME_EXTRA_REQUIREMENTS: Final[
    dict[str, _RequirementSpec]
] = {
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
    def netcdf_available(cls) -> bool:
        """Return whether netCDF support dependencies are installed."""
        return cls.package_available('xarray') and (
            cls.package_available('netCDF4')
            or cls.package_available('h5netcdf')
        )

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
            cls.package_available(module_name)
            for module_name in requirement.modules
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

    @classmethod
    def connector_gap_rows(
        cls,
        cfg: Config,
    ) -> list[dict[str, str]]:
        """Return connector configuration gaps that will block execution."""
        gaps: list[dict[str, str]] = []
        for role, connector in cls.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            connector_type = str(getattr(connector, 'type', ''))
            coerced_type = cls.connector_type(connector_type)

            if coerced_type is None:
                gaps.append(
                    {
                        'connector': connector_name,
                        'issue': f'unsupported type: {connector_type or "<empty>"}',
                        'role': role,
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
                                f'{format_name} format requires '
                                f'{requirement.package}'
                            ),
                            requirement=requirement,
                            role=role,
                        ),
                    )

        return cls.dedupe_rows(rows)

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
                        'runtime fields.'
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
    def config_checks(
        cls,
        config_path: str,
        *,
        env: Mapping[str, str] | None,
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

        cfg = Config.from_dict(raw)
        base_env = dict(getattr(cfg.profile, 'env', {}) or {})
        external_env = dict(env) if env is not None else dict(os.environ)
        effective_env = base_env | external_env
        resolved = deep_substitute(raw, cfg.vars, effective_env)
        unresolved = sorted(cls.collect_substitution_tokens(resolved))

        if unresolved:
            checks.append(
                cls.make_check(
                    'config-substitution',
                    'error',
                    'Configuration still contains unresolved substitution tokens.',
                    unresolved_tokens=unresolved,
                ),
            )
            return checks

        resolved_cfg = Config.from_dict(cast(StrAnyMap, resolved))
        checks.append(
            cls.make_check(
                'config-substitution',
                'ok',
                'Configuration substitutions resolved successfully.',
            ),
        )
        checks.extend(cls.connector_readiness_checks(resolved_cfg))
        return checks

    @classmethod
    def build(
        cls,
        *,
        config_path: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Build a runtime readiness report for the current ETLPlus environment.

        Parameters
        ----------
        config_path : str | None, optional
            Optional pipeline configuration file to validate. Default is ``None``.
        env : Mapping[str, str] | None, optional
            Optional environment mapping used instead of :data:`os.environ`.

        Returns
        -------
        dict[str, Any]
            JSON-serializable readiness report.
        """
        checks: list[dict[str, Any]] = [cls.supported_python_check()]

        if config_path:
            try:
                checks.extend(cls.config_checks(config_path, env=env))
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
