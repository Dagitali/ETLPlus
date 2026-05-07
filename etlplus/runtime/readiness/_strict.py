"""
:mod:`etlplus.runtime.readiness._strict` module.

Strict configuration validation helpers for runtime readiness reports.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Any

from ...connector import parse_connector
from ...utils._types import StrAnyMap
from ...workflow import ScheduleConfig
from ...workflow import schedule_validation_issues
from . import _connectors

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'StrictConfigValidator',
]


# SECTION: CLASSES ========================================================== #


class StrictConfigValidator:
    """
    Validate tolerant config shapes and references in strict readiness mode.
    """

    # -- Static Methods -- #

    @staticmethod
    def config_issue_rows(
        *,
        raw: StrAnyMap,
        connector_type_guidance: Callable[[str], str] = (
            _connectors.connector_type_guidance
        ),
        connector_type_choices: Callable[[], tuple[str, ...]] = (
            _connectors.connector_type_choices
        ),
    ) -> list[dict[str, Any]]:
        """
        Return strict-mode config issues hidden by tolerant parsing.

        This method checks the configuration for issues that may not be caught
        by tolerant parsing, such as missing or invalid connector definitions.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration mapping.
        connector_type_guidance : Callable[[str], str], optional
            A function to provide guidance for connector types.
        connector_type_choices : Callable[[], tuple[str, ...]], optional
            A function to provide available connector type choices.

        Returns
        -------
        list[dict[str, Any]]
            A list of strict-mode config issues.
        """
        issues: list[dict[str, Any]] = []
        source_names = StrictConfigValidator.connector_names(
            raw=raw,
            section='sources',
            issues=issues,
            connector_type_guidance=connector_type_guidance,
            connector_type_choices=connector_type_choices,
        )
        target_names = StrictConfigValidator.connector_names(
            raw=raw,
            section='targets',
            issues=issues,
            connector_type_guidance=connector_type_guidance,
            connector_type_choices=connector_type_choices,
        )
        transform_names = StrictConfigValidator.named_section_names(
            raw=raw,
            section='transforms',
            issues=issues,
            guidance='Define transforms as a mapping keyed by pipeline name.',
        )
        validation_names = StrictConfigValidator.named_section_names(
            raw=raw,
            section='validations',
            issues=issues,
            guidance='Define validations as a mapping keyed by ruleset name.',
        )
        StrictConfigValidator.job_issue_rows(
            raw=raw,
            issues=issues,
            source_names=source_names,
            target_names=target_names,
            transform_names=transform_names,
            validation_names=validation_names,
        )
        StrictConfigValidator.schedule_issue_rows(
            raw=raw,
            issues=issues,
            job_names=StrictConfigValidator.job_names(raw=raw),
        )
        return issues

    @staticmethod
    def connector_names(
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
        connector_type_guidance: Callable[[str], str] = (
            _connectors.connector_type_guidance
        ),
        connector_type_choices: Callable[[], tuple[str, ...]] = (
            _connectors.connector_type_choices
        ),
    ) -> set[str] | None:
        """
        Validate connector entries in *section* and return known names.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration mapping.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any found issues.
        connector_type_guidance : Callable[[str], str], optional
            A function to provide guidance for connector types.
        connector_type_choices : Callable[[], tuple[str, ...]], optional
            A function to provide available connector type choices.

        Returns
        -------
        set[str] | None
            A set of known connector names, or None if the section is invalid.
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
                    guidance = connector_type_guidance(raw_type)
                elif raw_type is None:
                    guidance = (
                        'Set "type" to one of: '
                        + ', '.join(connector_type_choices())
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

    @staticmethod
    def job_issue_rows(
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

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration mapping.
        issues : list[dict[str, Any]]
            A list to append any found issues.
        source_names : set[str] | None
            A set of known source names.
        target_names : set[str] | None
            A set of known target names.
        transform_names : set[str] | None
            A set of known transform names.
        validation_names : set[str] | None
            A set of known validation names.
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

            StrictConfigValidator.job_ref_issue(
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
            StrictConfigValidator.job_ref_issue(
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
            StrictConfigValidator.job_ref_issue(
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
            StrictConfigValidator.job_ref_issue(
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

    @staticmethod
    def job_names(
        *,
        raw: StrAnyMap,
    ) -> set[str]:
        """Return known non-empty job names from the raw jobs section."""
        value = raw.get('jobs')
        if not isinstance(value, list):
            return set()
        return {
            name.strip()
            for entry in value
            if isinstance(entry, Mapping)
            and isinstance(name := entry.get('name'), str)
            and name.strip()
        }

    @staticmethod
    def job_ref_issue(
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

        Parameters
        ----------
        entry : Mapping[str, Any]
            The job entry to validate.
        field : str
            The field name to check.
        index : int
            The index of the job in the jobs list.
        issues : list[dict[str, Any]]
            A list to append any found issues.
        job_name : str | None
            The name of the job, if available.
        required : bool
            Whether the field is required.
        required_key : str
            The required key within the field.
        section_names : set[str] | None
            A set of known section names.
        section_label : str
            The label for the section.
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
                            f'Add a {field} mapping with "{required_key}" set to a '
                            'configured resource name.'
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
                        f'Define {field} as a mapping with a "{required_key}" '
                        'string field.'
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
                        f'Define "{ref_name}" under top-level "{section_label}" or '
                        f'update {field}.{required_key}.'
                    ),
                    'issue': f'unknown {section_label[:-1]} reference: {ref_name}',
                },
            )

    @staticmethod
    def named_section_names(
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
        guidance: str,
    ) -> set[str] | None:
        """
        Validate one mapping-like top-level section and return its keys.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration mapping.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any found issues.
        guidance : str
            Guidance message for the section.

        Returns
        -------
        set[str] | None
            The set of keys in the section, or None if the section is invalid.
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

    @staticmethod
    def schedule_issue_guidance(
        issue: str,
    ) -> str | None:
        """Return one guidance message for a strict schedule issue."""
        if issue.startswith('duplicate schedule name: '):
            return 'Use unique schedule names within schedules.'
        if issue == 'schedule must define exactly one target: cron or interval':
            return None
        if issue == 'schedule must define exactly one trigger: cron or interval':
            return 'Set exactly one of "cron" or "interval" for each schedule.'
        if issue == 'schedule must define a target':
            return 'Add a target mapping with either "job" or "run_all".'
        if issue == 'schedule target must define exactly one mode: job or run_all':
            return 'Set exactly one of target.job or target.run_all.'
        if issue.startswith('unknown scheduled job reference: '):
            job_name = issue.split(': ', maxsplit=1)[1]
            return f'Define "{job_name}" under top-level jobs or update target.job.'
        if issue == 'cron helper emission currently supports exactly five cron fields':
            return 'Use a five-field cron expression: minute hour day month weekday.'
        if (
            issue
            == (
                'cron helper emission currently supports '
                'only single values or "*" fields'
            )
        ):
            return (
                'Use single cron field values or "*" for helper-compatible schedules.'
            )
        return None

    @staticmethod
    def schedule_issue_rows(
        *,
        raw: StrAnyMap,
        issues: list[dict[str, Any]],
        job_names: set[str],
    ) -> None:
        """Append strict-mode schedule diagnostics to *issues*."""
        value = raw.get('schedules')
        if value is None:
            return
        if not isinstance(value, list):
            issues.append(
                {
                    'expected': 'list',
                    'guidance': 'Define schedules as a YAML list of schedule mappings.',
                    'issue': 'invalid section type',
                    'observed_type': type(value).__name__,
                    'section': 'schedules',
                },
            )
            return

        parsed_schedules: list[ScheduleConfig] = []
        for index, entry in enumerate(value):
            if not isinstance(entry, Mapping):
                issues.append(
                    {
                        'guidance': (
                            'Define each schedule as a mapping with "name" plus '
                            'portable trigger and target fields.'
                        ),
                        'index': index,
                        'issue': 'invalid schedule entry',
                        'observed_type': type(entry).__name__,
                        'section': 'schedules',
                    },
                )
                continue
            schedule = ScheduleConfig.from_obj(entry)
            if schedule is None:
                issues.append(
                    {
                        'guidance': 'Set "name" to a non-empty string.',
                        'index': index,
                        'issue': 'missing schedule name',
                        'section': 'schedules',
                    },
                )
                continue
            parsed_schedules.append(schedule)

        for issue_row in schedule_validation_issues(
            parsed_schedules,
            job_names=job_names,
        ):
            issue = issue_row['issue']
            issues.append(
                {
                    'guidance': StrictConfigValidator.schedule_issue_guidance(issue),
                    'issue': issue,
                    'schedule': issue_row['schedule'],
                    'section': 'schedules',
                },
            )
