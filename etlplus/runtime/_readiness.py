"""
:mod:`etlplus.runtime._readiness` module.

Runtime readiness checks for the CLI and future execution surfaces.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from importlib.util import find_spec
from pathlib import Path
from typing import Any
from typing import cast

from .. import __version__
from .._config import Config
from ..connector import DataConnectorType as _DataConnectorType
from ..file import File
from ..file import FileFormat
from ..utils import deep_substitute
from ..utils import maybe_mapping
from ..utils._types import StrAnyMap
from . import _readiness_checks as _checks
from . import _readiness_strict as _strict
from ._readiness_support import _SUPPORTED_PYTHON_RANGE
from ._readiness_support import _TOKEN_PATTERN
from ._readiness_support import CheckStatus
from ._readiness_support import _RequirementSpec
from ._readiness_support import _ResolvedConfigContext

DataConnectorType = _DataConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]


# SECTION: CLASSES ========================================================== #


class ReadinessReportBuilder:
    """Shared builder for ETLPlus runtime readiness reports."""

    # -- Shared Runtime/Connector Helpers -- #

    coerce_connector_storage_scheme = staticmethod(
        _checks.coerce_connector_storage_scheme,
    )
    coerce_storage_scheme = staticmethod(_checks.coerce_storage_scheme)
    connector_gap_guidance = staticmethod(_checks.connector_gap_guidance)
    dedupe_rows = staticmethod(_checks.dedupe_rows)
    iter_connectors = staticmethod(_checks.iter_connectors)
    missing_requirement_guidance = staticmethod(
        _checks.missing_requirement_guidance,
    )

    # -- Static Methods -- #

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
    def effective_environment(
        cfg: Config,
        env: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """Return the merged environment used for config substitution."""
        base_env = dict(getattr(cfg.profile, 'env', {}) or {})
        external_env = dict(env) if env is not None else dict(os.environ)
        return base_env | external_env

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
        return _checks.aws_env_hint_present(env)

    @classmethod
    def azure_authority_has_account_host(
        cls,
        path: str,
    ) -> bool:
        """Return whether one Azure storage path authority embeds an account host."""
        return _checks.azure_authority_has_account_host(path)

    @classmethod
    def connector_gap_rows(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """Return connector configuration gaps that will block execution."""
        return _checks.connector_gap_rows(cfg)

    @classmethod
    def connector_readiness_checks(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """Return connector configuration and dependency readiness checks."""
        return _checks.connector_readiness_checks(
            cfg,
            connector_gap_rows_fn=cls.connector_gap_rows,
            make_check=cls.make_check,
            missing_requirement_rows_fn=lambda inner_cfg: cls.missing_requirement_rows(
                cfg=inner_cfg,
            ),
        )

    @classmethod
    def connector_type(
        cls,
        connector_type: str,
    ) -> _DataConnectorType | None:
        """Return one coerced connector type or ``None`` when unsupported."""
        return _checks.connector_type(connector_type)

    @classmethod
    def connector_type_choices(
        cls,
    ) -> tuple[str, ...]:
        """Return the supported connector type names."""
        return _checks.connector_type_choices()

    @classmethod
    def connector_type_guidance(
        cls,
        connector_type: str,
    ) -> str:
        """Return actionable guidance for an unsupported connector type."""
        return _checks.connector_type_guidance(connector_type)

    @classmethod
    def explicit_aws_credential_gap(
        cls,
        env: Mapping[str, str],
    ) -> dict[str, Any] | None:
        """Return one AWS env error row for incomplete explicit credentials."""
        return _checks.explicit_aws_credential_gap(env)

    @classmethod
    def missing_requirement_rows(
        cls,
        *,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """Return missing optional dependency rows for configured connectors."""
        return _checks.missing_requirement_rows(
            cfg=cfg,
            netcdf_available_fn=cls.netcdf_available,
            requirement_available_fn=cls.requirement_available,
        )

    @classmethod
    def netcdf_available(cls) -> bool:
        """Return whether netCDF support dependencies are installed."""
        return _checks.netcdf_available(package_available=cls.package_available)

    @classmethod
    def provider_environment_checks(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """Return provider-specific environment readiness checks."""
        return _checks.provider_environment_checks(
            cfg=cfg,
            env=env,
            make_check=cls.make_check,
            provider_environment_rows_fn=lambda inner_cfg, inner_env: (
                cls.provider_environment_rows(cfg=inner_cfg, env=inner_env)
            ),
        )

    @classmethod
    def provider_environment_rows(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """Return provider-specific environment gaps for configured connectors."""
        return _checks.provider_environment_rows(cfg=cfg, env=env)

    @classmethod
    def requirement_available(
        cls,
        requirement: _RequirementSpec,
    ) -> bool:
        """Return whether any module for one requirement is importable."""
        return _checks.requirement_available(
            requirement,
            package_available=cls.package_available,
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
        """Return one missing-requirement row."""
        return _checks.requirement_row(
            connector=connector,
            detected_format=detected_format,
            detected_scheme=detected_scheme,
            reason=reason,
            requirement=requirement,
            role=role,
        )

    @classmethod
    def strict_config_issue_rows(
        cls,
        *,
        raw: StrAnyMap,
    ) -> list[dict[str, Any]]:
        """Return strict-mode config issues hidden by tolerant parsing."""
        return _strict.strict_config_issue_rows(
            raw=raw,
            connector_type_guidance=cls.connector_type_guidance,
            connector_type_choices=cls.connector_type_choices,
        )

    @classmethod
    def strict_connector_names(
        cls,
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
    ) -> set[str] | None:
        """Validate connector entries in *section* and return known names."""
        return _strict.strict_connector_names(
            raw=raw,
            section=section,
            issues=issues,
            connector_type_guidance=cls.connector_type_guidance,
            connector_type_choices=cls.connector_type_choices,
        )

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
        _strict.strict_job_issue_rows(
            raw=raw,
            issues=issues,
            source_names=source_names,
            target_names=target_names,
            transform_names=transform_names,
            validation_names=validation_names,
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
        _strict.strict_job_ref_issue(
            entry=entry,
            field=field,
            index=index,
            issues=issues,
            job_name=job_name,
            required=required,
            required_key=required_key,
            section_names=section_names,
            section_label=section_label,
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
        return _strict.strict_named_section_names(
            raw=raw,
            section=section,
            issues=issues,
            guidance=guidance,
        )

    @classmethod
    def build(
        cls,
        *,
        config_path: str | None = None,
        env: Mapping[str, str] | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """Build one runtime readiness report for the current ETLPlus process."""
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
                        'No configuration file provided; only runtime checks '
                        'were performed.'
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

        checks: list[dict[str, Any]] = [
            cls.make_check(
                'config-file',
                'ok',
                f'Configuration file exists: {path}',
                path=str(path),
            ),
        ]
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
                            'Strict config validation found malformed or inconsistent '
                            'configuration entries.'
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
    def overall_status(
        cls,
        checks: list[dict[str, Any]],
    ) -> CheckStatus:
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
    def supported_python_check(
        cls,
    ) -> dict[str, Any]:
        """Return the runtime Python compatibility check."""
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
                f'Python {version} is outside the supported ETLPlus runtime range '
                f'(>={minimum[0]}.{minimum[1]},<{maximum[0]}.{maximum[1]}).'
            ),
            version=version,
        )

    @classmethod
    def token_reference_rows(
        cls,
        value: Any,
    ) -> list[dict[str, Any]]:
        """Return unresolved token rows with stable dotted and indexed paths."""
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
            {'name': name, 'paths': sorted(paths)}
            for name, paths in sorted(paths_by_name.items())
        ]
