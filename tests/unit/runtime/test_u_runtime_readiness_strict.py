"""
:mod:`tests.unit.runtime.test_u_runtime_readiness_strict` module.

Strict-structure readiness unit tests for :mod:`etlplus.runtime.readiness._builder`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime.readiness._strict as readiness_strict_mod

from .pytest_runtime_readiness import build_issue_row as _issue

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilderStrict:
    """Strict-structure unit tests for :class:`ReadinessReportBuilder`."""

    def test_strict_schedule_issue_rows_reject_non_list_section(
        self,
    ) -> None:
        """Strict schedule validation should reject non-list schedule sections."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.schedule_issue_rows(
            raw={'schedules': {'name': 'nightly_all'}},
            issues=issues,
            job_names=set(),
        )

        assert issues == [
            _issue(
                expected='list',
                guidance='Define schedules as a YAML list of schedule mappings.',
                issue='invalid section type',
                observed_type='dict',
                section='schedules',
            ),
        ]

    def test_strict_config_issue_rows_report_duplicates_and_unknown_refs(
        self,
    ) -> None:
        """Strict issue rows should surface hidden connector/job problems."""
        issues = readiness_strict_mod.StrictConfigValidator.config_issue_rows(
            raw={
                'sources': [
                    {
                        'name': 'src',
                        'type': 'file',
                        'format': 'json',
                        'path': 'input.json',
                    },
                    {'name': 'src', 'type': 'file'},
                ],
                'targets': [
                    {
                        'name': 'dest',
                        'type': 'file',
                        'format': 'json',
                        'path': 'out.json',
                    },
                ],
                'transforms': {},
                'jobs': [
                    {
                        'name': 'publish',
                        'extract': {'source': 'src'},
                        'transform': {'pipeline': 'missing-pipeline'},
                        'load': {'target': 'dest'},
                    },
                ],
            },
        )

        assert any(
            issue['issue'] == 'duplicate connector name: src' for issue in issues
        )
        assert any(
            issue['issue'] == 'unknown transform reference: missing-pipeline'
            for issue in issues
        )

    def test_strict_config_issue_rows_report_schedule_semantic_problems(
        self,
    ) -> None:
        """Strict issue rows should surface schedule semantic problems."""
        issues = readiness_strict_mod.StrictConfigValidator.config_issue_rows(
            raw={
                'jobs': [
                    {
                        'name': 'publish',
                        'extract': {'source': 'src'},
                        'load': {'target': 'dest'},
                    },
                ],
                'sources': [
                    {
                        'name': 'src',
                        'type': 'file',
                        'format': 'json',
                        'path': 'in.json',
                    },
                ],
                'targets': [
                    {
                        'name': 'dest',
                        'type': 'file',
                        'format': 'json',
                        'path': 'out.json',
                    },
                ],
                'schedules': [
                    {
                        'name': 'nightly_all',
                        'cron': '*/15 * * * *',
                        'target': {'job': 'missing-job'},
                    },
                ],
            },
        )

        assert any(
            issue['issue'] == 'unknown scheduled job reference: missing-job'
            and issue['section'] == 'schedules'
            for issue in issues
        )
        assert any(
            issue['issue']
            == (
                'cron helper emission currently supports '
                'only single values or "*" fields'
            )
            and issue['section'] == 'schedules'
            for issue in issues
        )

    def test_strict_schedule_issue_rows_report_shape_problems(
        self,
    ) -> None:
        """Strict schedule validation should surface malformed schedule entries."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.schedule_issue_rows(
            raw={
                'schedules': [
                    'not-a-mapping',
                    {'cron': '0 2 * * *', 'target': {'run_all': True}},
                ],
            },
            issues=issues,
            job_names=set(),
        )

        assert issues == [
            _issue(
                guidance=(
                    'Define each schedule as a mapping with "name" plus '
                    'portable trigger and target fields.'
                ),
                index=0,
                issue='invalid schedule entry',
                observed_type='str',
                section='schedules',
            ),
            _issue(
                guidance='Set "name" to a non-empty string.',
                index=1,
                issue='missing schedule name',
                section='schedules',
            ),
        ]

    def test_strict_connector_names_report_parse_errors_and_blank_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Strict connector validation should surface parse errors and blanks."""
        issues: list[dict[str, Any]] = []

        def _parse_connector(entry: Mapping[str, object]) -> object:
            if entry.get('type') == 'weird':
                raise TypeError('bad connector')
            return SimpleNamespace(name='   ')

        monkeypatch.setattr(
            readiness_strict_mod,
            'parse_connector',
            _parse_connector,
        )

        names = readiness_strict_mod.StrictConfigValidator.connector_names(
            raw={
                'sources': [
                    {'type': 'weird'},
                    {'type': 'file'},
                ],
            },
            section='sources',
            issues=issues,
        )

        assert names == set()
        assert issues == [
            _issue(
                guidance=(
                    'Use one of the supported connector types: '
                    'api, database, file, queue.'
                ),
                index=0,
                issue='invalid connector entry',
                message='bad connector',
                section='sources',
            ),
            _issue(
                guidance='Set "name" to a non-empty string.',
                index=1,
                issue='blank connector name',
                section='sources',
            ),
        ]

    @pytest.mark.parametrize(
        ('raw', 'expected_names', 'expected_issues'),
        [
            pytest.param(
                {
                    'sources': [
                        {
                            'name': 'warehouse_bigquery',
                            'type': 'database',
                            'provider': 'bigquery',
                            'project': 'analytics-project',
                        },
                    ],
                },
                {'warehouse_bigquery'},
                [
                    _issue(
                        connector='warehouse_bigquery',
                        guidance=(
                            'Set "connection_string" to a database DSN or '
                            'SQLAlchemy-style URL, or define both "project" '
                            'and "dataset" for this BigQuery connector.'
                        ),
                        index=0,
                        issue='missing connection_string or bigquery project/dataset',
                        missing_fields=['dataset'],
                        provider='bigquery',
                        section='sources',
                    ),
                ],
                id='bigquery-provider-gap',
            ),
            pytest.param(
                {
                    'sources': [
                        {
                            'name': 'warehouse_snowflake',
                            'type': 'database',
                            'provider': 'snowflake',
                            'account': 'acme.us-east-1',
                            'database': 'ANALYTICS',
                        },
                    ],
                },
                {'warehouse_snowflake'},
                [
                    _issue(
                        connector='warehouse_snowflake',
                        guidance=(
                            'Set "connection_string" to a database DSN or '
                            'SQLAlchemy-style URL, or define "account", '
                            '"database", and "schema" for this Snowflake '
                            'connector.'
                        ),
                        index=0,
                        issue=(
                            'missing connection_string '
                            'or snowflake account/database/schema'
                        ),
                        missing_fields=['schema'],
                        provider='snowflake',
                        section='sources',
                    ),
                ],
                id='snowflake-provider-gap',
            ),
        ],
    )
    def test_strict_connector_names_report_provider_specific_metadata_gaps(
        self,
        raw: dict[str, object],
        expected_names: set[str],
        expected_issues: list[dict[str, object]],
    ) -> None:
        """Strict connector validation should surface provider metadata gaps."""
        issues: list[dict[str, Any]] = []

        names = readiness_strict_mod.StrictConfigValidator.connector_names(
            raw=raw,
            section='sources',
            issues=issues,
        )

        assert names == expected_names
        assert cast(list[dict[str, object]], issues) == expected_issues

    @pytest.mark.parametrize(
        (
            'parse_connector',
            'raw',
            'expected_names',
            'expected_issues',
        ),
        [
            pytest.param(
                None,
                {},
                set(),
                [],
                id='missing-section-returns-empty-set',
            ),
            pytest.param(
                lambda entry: (_ for _ in ()).throw(TypeError('unsupported connector')),
                {'sources': [{'type': 1}]},
                set(),
                [
                    _issue(
                        guidance=None,
                        index=0,
                        issue='invalid connector entry',
                        message='unsupported connector',
                        section='sources',
                    ),
                ],
                id='non-string-type-guidance-omitted',
            ),
            pytest.param(
                None,
                {'sources': {'name': 'not-a-list'}},
                None,
                [
                    _issue(
                        expected='list',
                        guidance='Define sources as a YAML list of connector mappings.',
                        issue='invalid section type',
                        observed_type='dict',
                        section='sources',
                    ),
                ],
                id='invalid-section-type',
            ),
            pytest.param(
                None,
                {'sources': ['not-a-mapping']},
                set(),
                [
                    _issue(
                        guidance=(
                            'Define each connector as a mapping with at least '
                            '"name" and "type" fields.'
                        ),
                        index=0,
                        issue='invalid connector entry',
                        observed_type='str',
                        section='sources',
                    ),
                ],
                id='invalid-connector-entry-type',
            ),
            pytest.param(
                lambda entry: (_ for _ in ()).throw(
                    TypeError('missing connector type'),
                ),
                {'sources': [{}]},
                set(),
                [
                    _issue(
                        guidance=('Set "type" to one of: api, database, file, queue.'),
                        index=0,
                        issue='invalid connector entry',
                        message='missing connector type',
                        section='sources',
                    ),
                ],
                id='missing-type-guidance',
            ),
        ],
    )
    def test_strict_connector_names_single_issue_cases(
        self,
        monkeypatch: pytest.MonkeyPatch,
        parse_connector: object,
        raw: dict[str, object],
        expected_names: set[str] | None,
        expected_issues: list[dict[str, object]],
    ) -> None:
        """Strict connector validation should emit the expected single-issue rows."""
        issues: list[dict[str, Any]] = []

        if parse_connector is not None:
            monkeypatch.setattr(
                readiness_strict_mod,
                'parse_connector',
                parse_connector,
            )

        names = readiness_strict_mod.StrictConfigValidator.connector_names(
            raw=raw,
            section='sources',
            issues=issues,
        )

        assert names == expected_names
        assert cast(list[dict[str, object]], issues) == expected_issues

    def test_strict_job_issue_rows_cover_non_list_invalid_entries_and_duplicates(
        self,
    ) -> None:
        """Strict job validation should cover malformed top-level and entry cases."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.job_issue_rows(
            raw={
                'jobs': [
                    'not-a-mapping',
                    {
                        'name': 'dup',
                        'extract': {'source': 'src'},
                        'load': {'target': 'dst'},
                    },
                    {
                        'name': 'dup',
                        'extract': {'source': 'src'},
                        'load': {'target': 'dst'},
                    },
                    {'extract': {'source': 'src'}, 'load': {'target': 'dst'}},
                ],
            },
            issues=issues,
            source_names={'src'},
            target_names={'dst'},
            transform_names=set(),
            validation_names=set(),
        )

        assert any(issue['issue'] == 'invalid job entry' for issue in issues)
        assert any(issue['issue'] == 'duplicate job name: dup' for issue in issues)
        assert any(issue['issue'] == 'missing job name' for issue in issues)

    def test_strict_job_issue_rows_reject_non_list_jobs_section(
        self,
    ) -> None:
        """Strict job validation should reject non-list jobs sections."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.job_issue_rows(
            raw={'jobs': {'name': 'publish'}},
            issues=issues,
            source_names=set(),
            target_names=set(),
            transform_names=set(),
            validation_names=set(),
        )

        assert issues == [
            _issue(
                expected='list',
                guidance='Define jobs as a YAML list of job mappings.',
                issue='invalid section type',
                observed_type='dict',
                section='jobs',
            ),
        ]

    def test_strict_job_issue_rows_return_when_jobs_section_is_missing(
        self,
    ) -> None:
        """Strict job validation should do nothing when jobs are absent."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.job_issue_rows(
            raw={},
            issues=issues,
            source_names=set(),
            target_names=set(),
            transform_names=set(),
            validation_names=set(),
        )

        assert not issues

    def test_strict_job_names_skip_non_list_and_blank_entries(self) -> None:
        """Strict job-name collection should ignore non-list and blank entries."""
        assert readiness_strict_mod.StrictConfigValidator.job_names(raw={}) == set()
        assert readiness_strict_mod.StrictConfigValidator.job_names(
            raw={
                'jobs': [
                    {'name': ' publish '},
                    {'name': '   '},
                    {'name': None},
                    'bad-entry',
                ],
            },
        ) == {'publish'}

    @pytest.mark.parametrize(
        (
            'entry',
            'field',
            'index',
            'job_name',
            'required',
            'required_key',
            'section_names',
            'section_label',
            'expected',
        ),
        [
            pytest.param(
                {},
                'extract',
                0,
                'publish',
                True,
                'source',
                {'src'},
                'sources',
                [
                    _issue(
                        field='extract',
                        guidance=(
                            'Add a extract mapping with "source" set to a '
                            'configured resource name.'
                        ),
                        index=0,
                        issue='missing extract section',
                        job='publish',
                        section='jobs',
                    ),
                ],
                id='missing-required-section',
            ),
            pytest.param(
                {'extract': 'src'},
                'extract',
                1,
                'publish',
                True,
                'source',
                {'src'},
                'sources',
                [
                    _issue(
                        field='extract',
                        guidance=(
                            'Define extract as a mapping with a "source" string field.'
                        ),
                        index=1,
                        issue='invalid extract section',
                        job='publish',
                        observed_type='str',
                        section='jobs',
                    ),
                ],
                id='invalid-extract-section',
            ),
            pytest.param(
                {'transform': {'pipeline': '   '}},
                'transform',
                2,
                None,
                False,
                'pipeline',
                {'trim'},
                'transforms',
                [
                    _issue(
                        field='transform.pipeline',
                        guidance=(
                            'Set transform.pipeline to a configured resource name.'
                        ),
                        index=2,
                        issue='missing transform.pipeline',
                        section='jobs',
                    ),
                ],
                id='missing-transform-pipeline',
            ),
        ],
    )
    def test_strict_job_ref_issue_reports_expected_issue(
        self,
        entry: dict[str, object],
        field: str,
        index: int,
        job_name: str | None,
        required: bool,
        required_key: str,
        section_names: set[str] | None,
        section_label: str,
        expected: list[dict[str, object]],
    ) -> None:
        """Strict job refs should emit the expected issue row for each case."""
        issues: list[dict[str, Any]] = []

        readiness_strict_mod.StrictConfigValidator.job_ref_issue(
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

        assert cast(list[dict[str, object]], issues) == expected

    def test_strict_named_section_names_reject_non_mapping_sections(
        self,
    ) -> None:
        """Strict named section validation should reject non-mapping values."""
        issues: list[dict[str, Any]] = []

        names = readiness_strict_mod.StrictConfigValidator.named_section_names(
            raw={'transforms': ['trim']},
            section='transforms',
            issues=issues,
            guidance='Define transforms as a mapping keyed by pipeline name.',
        )

        assert names is None
        assert issues == [
            _issue(
                expected='mapping',
                guidance='Define transforms as a mapping keyed by pipeline name.',
                issue='invalid section type',
                observed_type='list',
                section='transforms',
            ),
        ]

    @pytest.mark.parametrize(
        ('issue', 'expected'),
        [
            pytest.param(
                'duplicate schedule name: nightly',
                'Use unique schedule names within schedules.',
                id='duplicate-name',
            ),
            pytest.param(
                'schedule must define exactly one target: cron or interval',
                None,
                id='conflicting-targets',
            ),
            pytest.param(
                'schedule must define exactly one trigger: cron or interval',
                'Set exactly one of "cron" or "interval" for each schedule.',
                id='missing-trigger',
            ),
            pytest.param(
                'schedule must define a target',
                'Add a target mapping with either "job" or "run_all".',
                id='missing-target',
            ),
            pytest.param(
                'schedule target must define exactly one mode: job or run_all',
                'Set exactly one of target.job or target.run_all.',
                id='invalid-target-mode',
            ),
            pytest.param(
                'unknown scheduled job reference: sync-db',
                'Define "sync-db" under top-level jobs or update target.job.',
                id='unknown-job-reference',
            ),
            pytest.param(
                'cron helper emission currently supports exactly five cron fields',
                'Use a five-field cron expression: minute hour day month weekday.',
                id='unsupported-cron-length',
            ),
            pytest.param(
                (
                    'cron helper emission currently supports only single values '
                    'or "*" fields'
                ),
                (
                    'Use single cron field values or "*" for '
                    'helper-compatible schedules.'
                ),
                id='unsupported-cron-token',
            ),
            pytest.param('unknown issue', None, id='fallback-none'),
        ],
    )
    def test_strict_schedule_issue_guidance_covers_known_messages(
        self,
        issue: str,
        expected: str | None,
    ) -> None:
        """Strict schedule guidance should cover the known issue messages."""
        assert (
            readiness_strict_mod.StrictConfigValidator.schedule_issue_guidance(issue)
            == expected
        )
