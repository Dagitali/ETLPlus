"""
:mod:`etlplus.runtime.readiness._builder` module.

Runtime readiness checks for the CLI and future execution surfaces.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast

from ... import __version__
from ..._config import Config
from ...utils._types import StrAnyMap
from . import _connectors
from . import _providers
from . import _strict
from ._base import ReadinessBaseMixin
from ._support import RequirementSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]


# SECTION: CLASSES ========================================================== #


class ReadinessReportBuilder(ReadinessBaseMixin):
    """Shared builder for ETLPlus runtime readiness reports."""

    # -- Class Methods -- #

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
            The configuration object containing the connectors.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the readiness checks.
        """
        return _connectors.connector_readiness_checks(
            cfg,
            connector_gap_rows_fn=_connectors.connector_gap_rows,
            make_check=cls.make_check,
            missing_requirement_rows_fn=lambda inner_cfg: cls.missing_requirement_rows(
                cfg=inner_cfg,
            ),
        )

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
            The configuration object containing the connectors.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the missing optional dependencies
            for the configured connectors.
        """
        return _connectors.missing_requirement_rows(
            cfg=cfg,
            netcdf_available_fn=cls.netcdf_available,
            requirement_available_fn=cls.requirement_available,
        )

    @classmethod
    def netcdf_available(cls) -> bool:
        """
        Return whether netCDF support dependencies are installed.

        Returns
        -------
        bool
            True if netCDF support dependencies are installed, False otherwise.
        """
        return _connectors.netcdf_available(package_available=cls.package_available)

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
            The configuration object containing the connectors and profile.
        env : Mapping[str, str]
            The environment variables to consider for provider readiness.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the provider-specific
            environment readiness checks.

        """
        return _providers.provider_environment_checks(
            cfg=cfg,
            env=env,
            make_check=cls.make_check,
            provider_environment_rows_fn=lambda inner_cfg, inner_env: (
                _providers.provider_environment_rows(cfg=inner_cfg, env=inner_env)
            ),
        )

    @classmethod
    def requirement_available(
        cls,
        requirement: RequirementSpec,
    ) -> bool:
        """
        Return whether any module for one requirement is importable.

        Parameters
        ----------
        requirement : _RequirementSpec
            The requirement specification to check.

        Returns
        -------
        bool
            ``True`` if any module for the requirement is importable,
            ``False`` if not.
        """
        return _connectors.requirement_available(
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
        requirement: RequirementSpec,
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
            The raw configuration data.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the strict-mode config issues.
        """
        return _strict.strict_config_issue_rows(
            raw=raw,
            connector_type_guidance=_connectors.connector_type_guidance,
            connector_type_choices=_connectors.connector_type_choices,
        )

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

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any issues found.

        Returns
        -------
        set[str] | None
            A set of known connector names, or None if validation fails.
        """
        return _strict.strict_connector_names(
            raw=raw,
            section=section,
            issues=issues,
            connector_type_guidance=_connectors.connector_type_guidance,
            connector_type_choices=_connectors.connector_type_choices,
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
        """Append strict-mode job diagnostics to *issues*.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        source_names : set[str] | None
            A set of known source names, or None if validation fails.
        target_names : set[str] | None
            A set of known target names, or None if validation fails.
        transform_names : set[str] | None
            A set of known transform names, or None if validation fails.
        validation_names : set[str] | None
            A set of known validation names, or None if validation fails.
        """
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
        """Append one strict-mode job reference issue when needed.

        Parameters
        ----------
        entry : Mapping[str, Any]
            The job entry to validate.
        field : str
            The field name to validate.
        index : int
            The index of the job entry.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        job_name : str | None
            The name of the job, or None if not available.
        required : bool
            Whether the field is required.
        required_key : str
            The key that is required.
        section_names : set[str] | None
            A set of known section names, or None if validation fails.
        section_label : str
            The label of the section.
        """
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
        """Validate one mapping-like top-level section and return its keys.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        guidance : str
            Guidance message for the validation.

        Returns
        -------
        set[str] | None
            A set of known section names, or None if validation fails.
        """
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
        """
        Build one runtime readiness report for the current ETLPlus process.

        Parameters
        ----------
        config_path : str | None
            The path to the configuration file, or None if not provided.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.
        strict : bool
            Whether to perform strict validation.

        Returns
        -------
        dict[str, Any]
            A dictionary containing the readiness report.
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
        """
        Return readiness checks for one pipeline config path.

        Parameters
        ----------
        config_path : str
            The path to the configuration file.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.
        strict : bool
            Whether to perform strict validation.
        include_runtime_checks : bool
            Whether to include runtime checks.

        Returns
        -------
        list[dict[str, Any]]
            A list of readiness checks.
        """
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
    def strict_config_report(
        cls,
        *,
        config_path: str,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Return one strict config-validation report for the CLI command
        ``etlplus check --strict``.

        Parameters
        ----------
        config_path : str
            The path to the configuration file.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.

        Returns
        -------
        dict[str, Any]
            The strict config-validation report.
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
