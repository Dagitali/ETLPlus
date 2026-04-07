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

import etlplus.runtime.readiness._builder as readiness_mod
import etlplus.runtime.readiness._strict as readiness_strict_mod

from .pytest_runtime_readiness import build_issue_row as _issue

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilderStrict:
    """Strict-structure unit tests for :class:`ReadinessReportBuilder`."""

    def test_strict_config_issue_rows_report_duplicates_and_unknown_refs(
        self,
    ) -> None:
        """Strict issue rows should surface hidden connector/job problems."""
        issues = readiness_mod.ReadinessReportBuilder.strict_config_issue_rows(
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

        names = readiness_mod.ReadinessReportBuilder.strict_connector_names(
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
                    'Use one of the supported connector types: api, database, file.'
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
        (
            'parse_connector',
            'raw',
            'expected_names',
            'expected_issues',
        ),
        [
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
                lambda entry: (_ for _ in ()).throw(
                    TypeError('missing connector type'),
                ),
                {'sources': [{}]},
                set(),
                [
                    _issue(
                        guidance='Set "type" to one of: api, database, file.',
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

        names = readiness_mod.ReadinessReportBuilder.strict_connector_names(
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

        readiness_mod.ReadinessReportBuilder.strict_job_issue_rows(
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

        readiness_mod.ReadinessReportBuilder.strict_job_issue_rows(
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

        readiness_mod.ReadinessReportBuilder.strict_job_issue_rows(
            raw={},
            issues=issues,
            source_names=set(),
            target_names=set(),
            transform_names=set(),
            validation_names=set(),
        )

        assert not issues

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

        readiness_mod.ReadinessReportBuilder.strict_job_ref_issue(
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

        names = readiness_mod.ReadinessReportBuilder.strict_named_section_names(
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
