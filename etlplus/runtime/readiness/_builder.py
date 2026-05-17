"""
:mod:`etlplus.runtime.readiness._builder` module.

Runtime readiness checks for the CLI and future execution surfaces.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast

from ...__version__ import __version__ as _ETLPLUS_VERSION
from ..._config import Config
from ...utils import TokenReferenceCollector
from ...workflow._schedule import schedule_validation_issues
from ._base import ReadinessBaseMixin
from ._connectors import ConnectorReadinessPolicy
from ._connectors import connector_type_choices
from ._connectors import connector_type_guidance
from ._providers import ProviderEnvironmentPolicy
from ._strict import StrictConfigValidator
from ._support import ReadinessReport

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]


# SECTION: CLASSES ========================================================== #


class ReadinessReportBuilder(ReadinessBaseMixin):
    """Shared builder for ETLPlus runtime readiness reports."""

    # -- Internal Class Methods -- #

    @classmethod
    def _report_payload(
        cls,
        *,
        checks: list[dict[str, Any]],
        python_version: str | None,
    ) -> dict[str, Any]:
        """Return one normalized readiness-report payload."""
        return ReadinessReport(
            checks=checks,
            etlplus_version=_ETLPLUS_VERSION,
            status=cls.overall_status(checks),
            python_version=python_version,
        ).to_payload()

    # -- Class Methods -- #

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
                checks.extend(cls.config_checks(config_path, env=env, strict=strict))
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

        return cls._report_payload(
            checks=checks,
            python_version=cls.python_version(),
        )

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
        if unresolved_tokens := context.unresolved_tokens:
            checks.append(
                cls.make_check(
                    'config-substitution',
                    'error',
                    'Configuration still contains unresolved substitution tokens.',
                    missing_env=unresolved_tokens,
                    references=TokenReferenceCollector.collect_rows(
                        context.resolved_raw,
                    ),
                    unresolved_tokens=unresolved_tokens,
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
            strict_issues = StrictConfigValidator.config_issue_rows(
                raw=context.resolved_raw,
                connector_type_guidance=connector_type_guidance,
                connector_type_choices=connector_type_choices,
            )
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

        schedule_issues = schedule_validation_issues(
            getattr(resolved_cfg, 'schedules', []),
            job_names={job.name for job in getattr(resolved_cfg, 'jobs', [])},
        )
        if schedule_issues:
            checks.append(
                cls.make_check(
                    'schedule-config',
                    'error',
                    'Schedule validation found unsupported or inconsistent entries.',
                    issues=schedule_issues,
                ),
            )
            return checks

        checks.extend(
            ConnectorReadinessPolicy.readiness_checks(
                resolved_cfg,
                connector_gap_rows_fn=ConnectorReadinessPolicy.gap_rows,
                make_check=cls.make_check,
                package_available=cls.package_available,
            ),
        )
        checks.extend(
            ProviderEnvironmentPolicy.environment_checks(
                cfg=resolved_cfg,
                env=context.effective_env,
                make_check=cls.make_check,
                provider_environment_rows_fn=ProviderEnvironmentPolicy.environment_rows,
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
        return cls._report_payload(checks=checks, python_version=None)
