"""
:mod:`etlplus.runtime._readiness_strict` module.

Strict configuration validation helpers for runtime readiness reports.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Any

from ..connector import parse_connector
from ..utils._types import StrAnyMap
from . import _readiness_connectors as _connectors

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'strict_config_issue_rows',
    'strict_connector_names',
    'strict_job_issue_rows',
    'strict_job_ref_issue',
    'strict_named_section_names',
]


# SECTION: FUNCTIONS ======================================================== #


def strict_config_issue_rows(
    *,
    raw: StrAnyMap,
    connector_type_guidance: Callable[[str], str] = _connectors.connector_type_guidance,
    connector_type_choices: Callable[[], tuple[str, ...]] = (
        _connectors.connector_type_choices
    ),
) -> list[dict[str, Any]]:
    """Return strict-mode config issues hidden by tolerant parsing."""
    issues: list[dict[str, Any]] = []
    source_names = strict_connector_names(
        raw=raw,
        section='sources',
        issues=issues,
        connector_type_guidance=connector_type_guidance,
        connector_type_choices=connector_type_choices,
    )
    target_names = strict_connector_names(
        raw=raw,
        section='targets',
        issues=issues,
        connector_type_guidance=connector_type_guidance,
        connector_type_choices=connector_type_choices,
    )
    transform_names = strict_named_section_names(
        raw=raw,
        section='transforms',
        issues=issues,
        guidance='Define transforms as a mapping keyed by pipeline name.',
    )
    validation_names = strict_named_section_names(
        raw=raw,
        section='validations',
        issues=issues,
        guidance='Define validations as a mapping keyed by ruleset name.',
    )
    strict_job_issue_rows(
        raw=raw,
        issues=issues,
        source_names=source_names,
        target_names=target_names,
        transform_names=transform_names,
        validation_names=validation_names,
    )
    return issues


def strict_connector_names(
    *,
    raw: StrAnyMap,
    section: str,
    issues: list[dict[str, Any]],
    connector_type_guidance: Callable[[str], str] = _connectors.connector_type_guidance,
    connector_type_choices: Callable[[], tuple[str, ...]] = (
        _connectors.connector_type_choices
    ),
) -> set[str] | None:
    """Validate connector entries in *section* and return known names."""
    value = raw.get(section)
    if value is None:
        return set()
    if not isinstance(value, list):
        issues.append(
            {
                'expected': 'list',
                'guidance': f'Define {section} as a YAML list of connector mappings.',
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
                        'Define each connector as a mapping with at least "name" '
                        'and "type" fields.'
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
                    'Set "type" to one of: ' + ', '.join(connector_type_choices()) + '.'
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


def strict_job_issue_rows(
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
                        'Define each job as a mapping with "name", "extract", '
                        'and "load" sections.'
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

        strict_job_ref_issue(
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
        strict_job_ref_issue(
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
        strict_job_ref_issue(
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
        strict_job_ref_issue(
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


def strict_job_ref_issue(
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
        'field': field if field in {'extract', 'load'} else f'{field}.{required_key}',
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
                    f'Define {field} as a mapping with a "{required_key}" string field.'
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


def strict_named_section_names(
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
