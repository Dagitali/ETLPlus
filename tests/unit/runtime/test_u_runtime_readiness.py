"""
:mod:`tests.unit.test_u_runtime_readiness` module.

Unit tests for :mod:`etlplus.runtime._readiness`.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime._readiness as readiness_mod
import etlplus.runtime._readiness_strict as readiness_strict_mod

from .pytest_runtime_readiness import build_connector_gap_row as _connector_gap
from .pytest_runtime_readiness import build_issue_row as _issue
from .pytest_runtime_readiness import (
    build_missing_requirement_row as _missing_requirement,
)
from .pytest_runtime_readiness import build_provider_check as _provider_check
from .pytest_runtime_readiness import build_provider_gap_row as _provider_gap
from .pytest_runtime_readiness import build_runtime_cfg as _cfg
from .pytest_runtime_readiness import (
    patch_config_resolution as _patch_config_resolution,
)
from .pytest_runtime_readiness import patch_file_read as _patch_file_read
from .pytest_runtime_readiness import write_pipeline_config

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilder:
    """Unit tests for :class:`ReadinessReportBuilder`."""

    def test_build_runtime_only_emits_python_and_skipped_config_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that runtime-only builds emit the stable non-config readiness rows.
        """
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'supported_python_check',
            lambda: {'name': 'python-version', 'status': 'ok'},
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'python_version',
            classmethod(lambda cls: '3.13.12'),
        )

        report = readiness_mod.ReadinessReportBuilder.build(env={})

        assert report == {
            'checks': [
                {'name': 'python-version', 'status': 'ok'},
                {
                    'message': (
                        'No configuration file provided; only runtime '
                        'checks were performed.'
                    ),
                    'name': 'config-file',
                    'status': 'skipped',
                },
            ],
            'etlplus_version': readiness_mod.__version__,
            'python_version': '3.13.12',
            'status': 'ok',
        }

    def test_build_passes_strict_flag_through_to_config_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Build should propagate strict mode into config checks."""
        captured: dict[str, object] = {}

        def _config_checks(
            config_path: str,
            *,
            env: Mapping[str, str] | None,
            strict: bool = False,
            include_runtime_checks: bool = True,
        ) -> list[dict[str, object]]:
            captured.update(
                {
                    'config_path': config_path,
                    'env': env,
                    'strict': strict,
                    'include_runtime_checks': include_runtime_checks,
                },
            )
            return [{'name': 'config-file', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'config_checks',
            _config_checks,
        )

        report = readiness_mod.ReadinessReportBuilder.build(
            config_path='pipeline.yml',
            env={'MODE': 'test'},
            strict=True,
        )

        assert report['status'] == 'ok'
        assert captured == {
            'config_path': 'pipeline.yml',
            'env': {'MODE': 'test'},
            'strict': True,
            'include_runtime_checks': True,
        }

    def test_build_wraps_config_check_exceptions(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that build converts config-check exceptions into error rows."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'config_checks',
            lambda _config_path, env=None: (_ for _ in ()).throw(TypeError('boom')),
        )

        report = readiness_mod.ReadinessReportBuilder.build(
            config_path='pipeline.yml',
            env={},
        )

        assert report['status'] == 'error'
        assert report['checks'][0]['name'] == 'python-version'
        assert report['checks'][1] == {
            'message': 'boom',
            'name': 'config-parse',
            'path': 'pipeline.yml',
            'status': 'error',
        }

    def test_coerce_connector_storage_scheme_handles_blank_and_invalid_values(
        self,
    ) -> None:
        """Test connector-storage coercion for blank, valid, and invalid text."""
        builder = readiness_mod.ReadinessReportBuilder
        assert builder.coerce_connector_storage_scheme('') is None
        assert builder.coerce_connector_storage_scheme('s3') == 's3'
        assert builder.coerce_connector_storage_scheme('not-a-real-scheme') is None

    def test_coerce_storage_scheme_handles_missing_and_unknown_schemes(
        self,
    ) -> None:
        """Test storage-scheme coercion for local, blank, known, and unknown."""
        builder = readiness_mod.ReadinessReportBuilder
        assert builder.coerce_storage_scheme('local/path.csv') is None
        assert builder.coerce_storage_scheme('://missing') is None
        assert builder.coerce_storage_scheme('s3://bucket/input.csv') == 's3'
        assert builder.coerce_storage_scheme('custom://bucket/input.csv') == 'custom'

    def test_collect_substitution_tokens_walks_nested_container_types(
        self,
    ) -> None:
        """Test token collection across mappings and sequence container types."""
        value = {
            'list': ['${LIST_TOKEN}'],
            'tuple': ('${TUPLE_TOKEN}',),
            'set': {'${SET_TOKEN}'},
            'frozen': frozenset({'${FROZEN_TOKEN}'}),
            'number': 1,
        }

        tokens = readiness_mod.ReadinessReportBuilder.collect_substitution_tokens(
            value,
        )

        assert tokens == {
            'FROZEN_TOKEN',
            'LIST_TOKEN',
            'SET_TOKEN',
            'TUPLE_TOKEN',
        }

    @pytest.mark.parametrize(
        ('issue', 'api_reference', 'expected'),
        [
            pytest.param(
                'unknown api reference: missing-api',
                None,
                'Define the referenced API under top-level "apis".',
                id='unknown-api-without-reference-name',
            ),
            pytest.param(
                'unhandled',
                None,
                None,
                id='unknown-issue',
            ),
        ],
    )
    def test_connector_gap_guidance_covers_fallback_paths(
        self,
        issue: str,
        api_reference: str | None,
        expected: str | None,
    ) -> None:
        """Connector-gap guidance should cover non-standard issue shapes."""
        assert (
            readiness_mod.ReadinessReportBuilder.connector_gap_guidance(
                api_reference=api_reference,
                issue=issue,
            )
            == expected
        )

    def test_config_checks_can_skip_runtime_checks_without_strict_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Config checks should return early when runtime checks are disabled."""
        config_path = write_pipeline_config(tmp_path)
        runtime_called = {'value': False}
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
        )

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            runtime_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_readiness_checks',
            _connector_readiness_checks,
        )

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
            include_runtime_checks=False,
        )

        assert runtime_called['value'] is False
        assert [check['name'] for check in checks] == [
            'config-file',
            'config-parse',
            'config-substitution',
        ]

    def test_config_checks_returns_missing_file_error(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that config checks return a config-file error for missing paths."""
        missing_path = tmp_path / 'missing.yml'

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(missing_path),
            env={},
        )

        assert checks == [
            {
                'message': f'Configuration file does not exist: {missing_path}',
                'name': 'config-file',
                'path': str(missing_path),
                'status': 'error',
            },
        ]

    def test_config_checks_success_path_adds_connector_and_provider_results(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test the resolved config path that appends connector/provider checks."""
        config_path = write_pipeline_config(tmp_path)
        resolved_cfg = _cfg()
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
            resolved_cfg=resolved_cfg,
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_readiness_checks',
            lambda _cfg: [{'name': 'connector-readiness', 'status': 'ok'}],
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'provider_environment_checks',
            lambda *, cfg, env: [{'name': 'provider-environment', 'status': 'ok'}],
        )

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
        )

        assert checks == [
            {
                'message': f'Configuration file exists: {config_path}',
                'name': 'config-file',
                'path': str(config_path),
                'status': 'ok',
            },
            {
                'message': 'Configuration YAML parsed successfully.',
                'name': 'config-parse',
                'status': 'ok',
            },
            {
                'message': 'Configuration substitutions resolved successfully.',
                'name': 'config-substitution',
                'status': 'ok',
            },
            {'name': 'connector-readiness', 'status': 'ok'},
            {'name': 'provider-environment', 'status': 'ok'},
        ]

    def test_config_checks_unresolved_substitutions_skip_connector_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test that unresolved substitutions short-circuit connector checks."""
        config_path = write_pipeline_config(tmp_path)

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'load_raw_config',
            lambda _path: {'profile': {'env': {}}, 'vars': {'token': 'value'}},
        )

        def _config_from_dict(raw: dict[str, object]) -> object:
            profile = raw.get('profile')
            profile_env = profile.get('env', {}) if isinstance(profile, dict) else {}
            return SimpleNamespace(
                profile=SimpleNamespace(env=profile_env),
                vars=raw.get('vars', {}),
            )

        monkeypatch.setattr(readiness_mod.Config, 'from_dict', _config_from_dict)
        monkeypatch.setattr(
            readiness_mod,
            'deep_substitute',
            lambda raw, _vars, _env: {'token': '${MISSING_TOKEN}'},
        )

        connector_checks_called = {'value': False}

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            connector_checks_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_readiness_checks',
            _connector_readiness_checks,
        )

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
        )

        assert connector_checks_called['value'] is False
        assert checks == [
            {
                'message': f'Configuration file exists: {config_path}',
                'name': 'config-file',
                'path': str(config_path),
                'status': 'ok',
            },
            {
                'message': 'Configuration YAML parsed successfully.',
                'name': 'config-parse',
                'status': 'ok',
            },
            {
                'message': (
                    'Configuration still contains unresolved substitution tokens.'
                ),
                'missing_env': ['MISSING_TOKEN'],
                'name': 'config-substitution',
                'references': [{'name': 'MISSING_TOKEN', 'paths': ['token']}],
                'status': 'error',
                'unresolved_tokens': ['MISSING_TOKEN'],
            },
        ]

    def test_config_checks_strict_structure_errors_short_circuit_runtime_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Strict config issues should stop before runtime readiness checks."""
        config_path = write_pipeline_config(tmp_path)
        resolved_raw = {
            'sources': ['bad-entry'],
            'jobs': [
                {
                    'name': 'publish',
                    'extract': {'source': 'missing-source'},
                    'load': {'target': 'missing-target'},
                },
            ],
        }
        _patch_config_resolution(
            monkeypatch,
            raw=resolved_raw,
            resolved_raw=resolved_raw,
        )
        runtime_called = {'value': False}

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            runtime_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_readiness_checks',
            _connector_readiness_checks,
        )

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
            strict=True,
        )

        assert runtime_called['value'] is False
        structure_check = next(
            check for check in checks if check['name'] == 'config-structure'
        )
        assert structure_check['status'] == 'error'
        assert any(
            issue['issue'] == 'invalid connector entry'
            for issue in structure_check['issues']
        )
        assert any(
            issue['issue'] == 'unknown source reference: missing-source'
            for issue in structure_check['issues']
        )

    def test_config_checks_strict_success_adds_config_structure_ok_row(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Strict config checks should emit an explicit ok row when clean."""
        config_path = write_pipeline_config(tmp_path)
        resolved_cfg = _cfg()
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
            resolved_cfg=resolved_cfg,
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'strict_config_issue_rows',
            lambda *, raw: [],
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_readiness_checks',
            lambda _cfg: [{'name': 'connector-readiness', 'status': 'ok'}],
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'provider_environment_checks',
            lambda *, cfg, env: [{'name': 'provider-environment', 'status': 'ok'}],
        )

        checks = readiness_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
            strict=True,
        )

        assert any(
            check
            == {
                'message': (
                    'Strict config validation found no malformed or '
                    'inconsistent configuration entries.'
                ),
                'name': 'config-structure',
                'status': 'ok',
            }
            for check in checks
        )

    def test_connector_gap_rows_continue_after_complete_database_connector(
        self,
    ) -> None:
        """
        A complete database connector should not prevent later gaps from being
        reported.
        """
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    connection_string='sqlite:///:memory:',
                    name='db-source',
                    type='database',
                ),
                SimpleNamespace(
                    name='file-source',
                    path=None,
                    type='file',
                ),
            ],
        )

        rows = readiness_mod.ReadinessReportBuilder.connector_gap_rows(cast(Any, cfg))

        assert rows == [
            _connector_gap(
                connector='file-source',
                guidance=(
                    'Set "path" to a local path or storage URI for this file connector.'
                ),
                issue='missing path',
                role='source',
                connector_type='file',
            ),
        ]

    def test_connector_gap_rows_cover_missing_required_connector_fields(
        self,
    ) -> None:
        """Test gap rows for missing path, API linkage, and DB connection data."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(name='file-source', path=None, type='file'),
                SimpleNamespace(name='api-source', api=None, type='api', url=None),
                SimpleNamespace(
                    name='api-ref-source',
                    api='missing-api',
                    type='api',
                    url=None,
                ),
            ],
            targets=[
                SimpleNamespace(
                    connection_string=None,
                    name='db-target',
                    type='database',
                ),
            ],
            apis={},
        )

        rows = readiness_mod.ReadinessReportBuilder.connector_gap_rows(cast(Any, cfg))

        assert rows == [
            _connector_gap(
                connector='file-source',
                guidance=(
                    'Set "path" to a local path or storage URI for this file connector.'
                ),
                issue='missing path',
                role='source',
                connector_type='file',
            ),
            _connector_gap(
                connector='api-source',
                guidance=(
                    'Set "url" to a reachable endpoint or "api" to a configured '
                    'top-level API name.'
                ),
                issue='missing url or api reference',
                role='source',
                connector_type='api',
            ),
            _connector_gap(
                connector='api-ref-source',
                guidance=(
                    'Define "missing-api" under top-level "apis" or update the '
                    'connector "api" reference.'
                ),
                issue='unknown api reference: missing-api',
                role='source',
                connector_type='api',
            ),
            _connector_gap(
                connector='db-target',
                guidance=(
                    'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
                ),
                issue='missing connection_string',
                role='target',
                connector_type='database',
            ),
        ]

    def test_connector_gap_rows_report_actionable_unsupported_type_details(
        self,
    ) -> None:
        """Test that unsupported connector types include actionable guidance."""
        cfg = SimpleNamespace(
            sources=[
                SimpleNamespace(
                    name='remote-source',
                    type='s3',
                ),
            ],
            targets=[],
            apis={},
        )

        rows = readiness_mod.ReadinessReportBuilder.connector_gap_rows(
            cast(Any, cfg),
        )

        assert rows == [
            _connector_gap(
                connector='remote-source',
                guidance=(
                    '"s3" is a storage scheme, not a connector type. '
                    'Use connector type "file" and keep the provider in '
                    'the path or URI scheme.'
                ),
                issue='unsupported type',
                role='source',
                supported_types=['api', 'database', 'file'],
                connector_type='s3',
            ),
        ]

    def test_connector_gap_rows_return_empty_for_complete_connectors(
        self,
    ) -> None:
        """Test that complete connector definitions produce no gap rows."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='file-source',
                    path='input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    name='api-url-source',
                    api=None,
                    type='api',
                    url='https://example.test/data',
                ),
                SimpleNamespace(
                    name='api-ref-source',
                    api='known-api',
                    type='api',
                    url=None,
                ),
            ],
            targets=[
                SimpleNamespace(
                    connection_string='sqlite:///:memory:',
                    name='db-target',
                    type='database',
                ),
                SimpleNamespace(
                    connection_string='sqlite:///other.db',
                    name='db-target-2',
                    type='database',
                ),
            ],
            apis={'known-api': object()},
        )

        rows = readiness_mod.ReadinessReportBuilder.connector_gap_rows(cast(Any, cfg))

        assert not rows

    def test_connector_gap_rows_tolerate_unexpected_coerced_type_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test defensive fallthrough when the connector-type seam misbehaves."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='weird-source',
                    path='input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    name='normal-source',
                    path='input.csv',
                    type='file',
                ),
            ],
        )
        calls = {'count': 0}

        def _connector_type(_connector_type: str) -> object:
            calls['count'] += 1
            if calls['count'] == 1:
                return object()
            return readiness_mod.DataConnectorType.FILE

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_type',
            _connector_type,
        )

        rows = readiness_mod.ReadinessReportBuilder.connector_gap_rows(cast(Any, cfg))

        assert not rows

    def test_connector_readiness_checks_report_all_ok_states(self) -> None:
        """Test readiness rows when gaps and optional dependency gaps are absent."""
        cfg = _cfg()

        checks = readiness_mod.ReadinessReportBuilder.connector_readiness_checks(
            cast(Any, cfg),
        )

        assert checks == [
            {
                'message': 'Configured connectors include the required runtime fields.',
                'name': 'connector-readiness',
                'status': 'ok',
            },
            {
                'message': (
                    'No missing optional dependencies were detected for '
                    'configured connectors.'
                ),
                'name': 'optional-dependencies',
                'status': 'ok',
            },
        ]

    def test_connector_readiness_checks_report_gap_and_dependency_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test readiness rows when connector and dependency errors exist."""
        cfg = _cfg()
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'connector_gap_rows',
            lambda _cfg: [{'connector': 'bad-source'}],
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'missing_requirement_rows',
            lambda *, cfg: [{'connector': 'bad-source', 'missing_package': 'boto3'}],
        )

        checks = readiness_mod.ReadinessReportBuilder.connector_readiness_checks(
            cast(Any, cfg),
        )

        assert checks == [
            {
                'gaps': [{'connector': 'bad-source'}],
                'message': (
                    'One or more configured connectors are missing required '
                    'runtime fields or use unsupported connector types.'
                ),
                'name': 'connector-readiness',
                'status': 'error',
            },
            {
                'message': (
                    'Configured connectors require optional dependencies that are '
                    'not installed.'
                ),
                'missing_requirements': [
                    {'connector': 'bad-source', 'missing_package': 'boto3'},
                ],
                'name': 'optional-dependencies',
                'status': 'error',
            },
        ]

    def test_connector_type_guidance_covers_blank_and_generic_invalid_values(
        self,
    ) -> None:
        """Test actionable guidance for blank and non-storage invalid types."""
        builder = readiness_mod.ReadinessReportBuilder
        assert builder.connector_type_guidance('') == (
            'Set type to one of: api, database, file.'
        )
        assert builder.connector_type_guidance('weird') == (
            'Use one of the supported connector types: api, database, file.'
        )

    def test_dedupe_rows_preserves_first_occurrence(self) -> None:
        """Test duplicate requirement rows are removed while keeping order."""
        row = {
            'connector': 'source-a',
            'role': 'source',
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'extra': 'storage',
        }

        rows = readiness_mod.ReadinessReportBuilder.dedupe_rows(
            [row, dict(row), {**row, 'connector': 'source-b'}],
        )

        assert rows == [row, {**row, 'connector': 'source-b'}]

    @pytest.mark.parametrize(
        ('env', 'expected'),
        [
            pytest.param(
                {
                    'AWS_ACCESS_KEY_ID': 'access-key',
                    'AWS_SECRET_ACCESS_KEY': 'secret-key',
                },
                None,
                id='complete-explicit-pair',
            ),
            pytest.param(
                {},
                None,
                id='no-explicit-credentials',
            ),
            pytest.param(
                {'AWS_SECRET_ACCESS_KEY': 'secret-key'},
                {
                    'guidance': (
                        'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
                        'remove the partial explicit credential env vars and rely on '
                        'AWS_PROFILE, shared config files, container credentials, or '
                        'instance metadata.'
                    ),
                    'missing_env': ['AWS_ACCESS_KEY_ID'],
                    'provider': 'aws-s3',
                    'reason': (
                        'Incomplete explicit AWS access-key configuration was detected '
                        'for this S3 path.'
                    ),
                    'severity': 'error',
                },
                id='secret-only',
            ),
        ],
    )
    def test_explicit_aws_credential_gap_covers_guard_and_missing_access_key_paths(
        self,
        env: dict[str, str],
        expected: dict[str, object] | None,
    ) -> None:
        """Explicit AWS credential diagnostics should cover all short-circuit cases."""
        assert (
            readiness_mod.ReadinessReportBuilder.explicit_aws_credential_gap(env)
            == expected
        )

    @pytest.mark.parametrize(
        ('payload', 'expected', 'match'),
        [
            pytest.param(
                ['not', 'a', 'mapping'],
                None,
                'mapping/object root',
                id='rejects-non-mapping-root',
            ),
            pytest.param(
                {'name': 'pipeline'},
                {'name': 'pipeline'},
                None,
                id='returns-mapping-root',
            ),
        ],
    )
    def test_load_raw_config_validates_mapping_root(
        self,
        monkeypatch: pytest.MonkeyPatch,
        payload: object,
        expected: dict[str, object] | None,
        match: str | None,
    ) -> None:
        """Raw config loading should enforce a mapping/object root."""
        _patch_file_read(monkeypatch, payload)

        if match is not None:
            with pytest.raises(TypeError, match=match):
                readiness_mod.ReadinessReportBuilder.load_raw_config(
                    'pipeline.yml',
                )
            return

        assert (
            readiness_mod.ReadinessReportBuilder.load_raw_config(
                'pipeline.yml',
            )
            == expected
        )

    def test_missing_requirement_guidance_returns_plain_install_hint_without_context(
        self,
    ) -> None:
        """Missing-dependency guidance should fall back to one plain install hint."""
        assert (
            readiness_mod.ReadinessReportBuilder.missing_requirement_guidance(
                package='tables',
                extra=None,
            )
            == 'Install tables.'
        )

    def test_missing_requirement_rows_cover_netcdf_and_format_specific_branches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test requirement rows for netCDF and format-specific extras."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
                SimpleNamespace(
                    format='nc',
                    name='nc-source',
                    path='input.nc',
                    type='file',
                ),
                SimpleNamespace(
                    format='rda',
                    name='rda-source',
                    path='input.rda',
                    type='file',
                ),
            ],
        )

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'requirement_available',
            lambda requirement: requirement.package == 'boto3',
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'netcdf_available',
            lambda: False,
        )

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert rows == [
            _missing_requirement(
                connector='nc-source',
                detected_format='nc',
                extra='file',
                guidance=(
                    'Install xarray plus one of netCDF4 or h5netcdf, or install '
                    'the ETLPlus "file" extra.'
                ),
                missing_package='xarray/netCDF4',
                reason='nc format requires xarray and netCDF4 or h5netcdf',
                role='source',
            ),
            _missing_requirement(
                connector='rda-source',
                detected_format='rda',
                extra='file',
                guidance=(
                    'Install pyreadr directly or install the ETLPlus "file" '
                    'extra. Required for "rda" file format.'
                ),
                missing_package='pyreadr',
                reason='rda format requires pyreadr',
                role='source',
            ),
        ]

    def test_missing_requirement_rows_respects_package_available_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based dependency checks still honor wrapper patches."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'package_available',
            lambda module_name: False if module_name == 'boto3' else True,
        )
        cfg = SimpleNamespace(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
            ],
            targets=[],
            apis={},
        )

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert rows == [
            _missing_requirement(
                connector='s3-source',
                detected_scheme='s3',
                extra='storage',
                guidance=(
                    'Install boto3 directly or install the ETLPlus "storage" '
                    'extra. Required for "s3" storage paths.'
                ),
                missing_package='boto3',
                reason='s3 storage path requires boto3',
                role='source',
            ),
        ]

    def test_missing_requirement_rows_return_empty_when_requirements_are_satisfied(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test requirement rows when connectors either omit paths or have deps."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='pathless-source',
                    path=None,
                    type='file',
                ),
                SimpleNamespace(
                    format='nc',
                    name='nc-source',
                    path='input.nc',
                    type='file',
                ),
                SimpleNamespace(
                    format='sav',
                    name='sav-source',
                    path='input.sav',
                    type='file',
                ),
            ],
        )

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'netcdf_available',
            lambda: True,
        )
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'requirement_available',
            lambda requirement: True,
        )

        rows = readiness_mod.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert not rows

    @pytest.mark.parametrize(
        ('available_modules', 'expected'),
        [
            ({'xarray', 'netCDF4'}, True),
            ({'xarray', 'h5netcdf'}, True),
            ({'h5netcdf'}, False),
        ],
    )
    def test_netcdf_available_requires_xarray_and_one_backend(
        self,
        monkeypatch: pytest.MonkeyPatch,
        available_modules: set[str],
        expected: bool,
    ) -> None:
        """Test netCDF availability resolution across supported backend combos."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'package_available',
            lambda module_name: module_name in available_modules,
        )

        assert readiness_mod.ReadinessReportBuilder.netcdf_available() is expected

    def test_overall_status_warn_when_no_errors_exist(self) -> None:
        """Test aggregate status when warnings exist without errors."""
        checks = [
            {'status': 'ok'},
            {'status': 'warn'},
        ]

        assert readiness_mod.ReadinessReportBuilder.overall_status(checks) == 'warn'

    def test_package_available_handles_find_spec_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test package availability failures when spec lookup raises."""
        monkeypatch.setattr(
            readiness_mod,
            'find_spec',
            lambda _module_name: (_ for _ in ()).throw(ValueError('boom')),
        )

        assert readiness_mod.ReadinessReportBuilder.package_available('broken') is False

    @pytest.mark.parametrize(
        ('rows', 'expected'),
        [
            pytest.param(
                [
                    _provider_gap(
                        connector='blob-source',
                        guidance=(
                            'Set AZURE_STORAGE_CONNECTION_STRING, set '
                            'AZURE_STORAGE_ACCOUNT_URL, or include the '
                            'account host in the path authority.'
                        ),
                        missing_env=[
                            'AZURE_STORAGE_CONNECTION_STRING',
                            'AZURE_STORAGE_ACCOUNT_URL',
                        ],
                        provider='azure-storage',
                        reason=(
                            'azure-blob path does not provide an account host '
                            'and no Azure storage bootstrap settings were '
                            'found.'
                        ),
                        role='source',
                        scheme='azure-blob',
                        severity='error',
                    ),
                ],
                [
                    _provider_check(
                        message='Provider environment gaps: 1 error(s), 0 warning(s).',
                        rows=[
                            _provider_gap(
                                connector='blob-source',
                                guidance=(
                                    'Set AZURE_STORAGE_CONNECTION_STRING, set '
                                    'AZURE_STORAGE_ACCOUNT_URL, or include the '
                                    'account host in the path authority.'
                                ),
                                missing_env=[
                                    'AZURE_STORAGE_CONNECTION_STRING',
                                    'AZURE_STORAGE_ACCOUNT_URL',
                                ],
                                provider='azure-storage',
                                reason=(
                                    'azure-blob path does not provide an account host '
                                    'and no Azure storage bootstrap settings were '
                                    'found.'
                                ),
                                role='source',
                                scheme='azure-blob',
                                severity='error',
                            ),
                        ],
                        status='error',
                    ),
                ],
                id='error-rows',
            ),
            pytest.param(
                [],
                [
                    _provider_check(
                        message='No provider-specific environment gaps were detected.',
                        status='ok',
                    ),
                ],
                id='no-rows',
            ),
            pytest.param(
                [
                    _provider_gap(
                        connector='s3-source',
                        guidance=(
                            'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                            'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                            'files, container credentials, or instance '
                            'metadata.'
                        ),
                        missing_env=[
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
                        ],
                        provider='aws-s3',
                        reason=(
                            'No common AWS credential-chain environment hints '
                            'were detected for this S3 path.'
                        ),
                        role='source',
                        scheme='s3',
                        severity='warn',
                    ),
                ],
                [
                    _provider_check(
                        message='Provider environment warnings: 1.',
                        rows=[
                            _provider_gap(
                                connector='s3-source',
                                guidance=(
                                    'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                                    'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                                    'files, container credentials, or instance '
                                    'metadata.'
                                ),
                                missing_env=[
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
                                ],
                                provider='aws-s3',
                                reason=(
                                    'No common AWS credential-chain environment hints '
                                    'were detected for this S3 path.'
                                ),
                                role='source',
                                scheme='s3',
                                severity='warn',
                            ),
                        ],
                        status='warn',
                    ),
                ],
                id='warn-rows',
            ),
        ],
    )
    def test_provider_environment_checks_wrap_rows_by_severity(
        self,
        monkeypatch: pytest.MonkeyPatch,
        rows: list[dict[str, object]],
        expected: list[dict[str, object]],
    ) -> None:
        """Provider check wrappers should map row severities into report rows."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'provider_environment_rows',
            lambda *, cfg, env: rows,
        )

        checks = readiness_mod.ReadinessReportBuilder.provider_environment_checks(
            cfg=cast(Any, _cfg()),
            env={},
        )

        assert checks == expected

    @pytest.mark.parametrize(
        ('cfg', 'env', 'expected'),
        [
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='blob-source',
                            format='csv',
                            path='azure-blob://container/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='blob-source',
                        guidance=(
                            'Set AZURE_STORAGE_CONNECTION_STRING, set '
                            'AZURE_STORAGE_ACCOUNT_URL, or include the '
                            'account host in the path authority.'
                        ),
                        missing_env=[
                            'AZURE_STORAGE_CONNECTION_STRING',
                            'AZURE_STORAGE_ACCOUNT_URL',
                        ],
                        provider='azure-storage',
                        reason=(
                            'azure-blob path does not provide an account host '
                            'and no Azure storage bootstrap settings were '
                            'found.'
                        ),
                        role='source',
                        scheme='azure-blob',
                        severity='error',
                    ),
                ],
                id='azure-bootstrap-error',
            ),
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='s3-source',
                            format='csv',
                            path='s3://bucket/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='s3-source',
                        guidance=(
                            'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                            'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                            'files, container credentials, or instance '
                            'metadata.'
                        ),
                        missing_env=[
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
                        ],
                        provider='aws-s3',
                        reason=(
                            'No common AWS credential-chain environment hints '
                            'were detected for this S3 path.'
                        ),
                        role='source',
                        scheme='s3',
                        severity='warn',
                    ),
                ],
                id='s3-credential-warning',
            ),
        ],
    )
    def test_provider_environment_rows_report_expected_gaps(
        self,
        cfg: object,
        env: dict[str, str],
        expected: list[dict[str, object]],
    ) -> None:
        """Provider row generation should emit the expected Azure/S3 gaps."""
        rows = readiness_mod.ReadinessReportBuilder.provider_environment_rows(
            cfg=cast(Any, cfg),
            env=env,
        )

        assert rows == expected

    def test_provider_environment_rows_report_incomplete_explicit_aws_credentials(
        self,
    ) -> None:
        """Partial explicit AWS credentials should produce one provider error."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
            ],
        )

        rows = readiness_mod.ReadinessReportBuilder.provider_environment_rows(
            cfg=cast(Any, cfg),
            env={'AWS_ACCESS_KEY_ID': 'access-key'},
        )

        assert rows == [
            {
                'connector': 's3-source',
                'guidance': (
                    'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
                    'remove the partial explicit credential env vars and rely on '
                    'AWS_PROFILE, shared config files, container credentials, or '
                    'instance metadata.'
                ),
                'missing_env': ['AWS_SECRET_ACCESS_KEY'],
                'provider': 'aws-s3',
                'reason': (
                    'Incomplete explicit AWS access-key configuration was detected '
                    'for this S3 path.'
                ),
                'role': 'source',
                'scheme': 's3',
                'severity': 'error',
            },
        ]

    @pytest.mark.parametrize(
        ('cfg', 'env'),
        [
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='blob-source',
                            path='azure-blob://container/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {
                    'AZURE_STORAGE_ACCOUNT_URL': (
                        'https://account.blob.core.windows.net'
                    ),
                    'AZURE_STORAGE_CREDENTIAL': 'secret',
                },
                id='explicit-azure-auth',
            ),
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='s3-source',
                            path='s3://bucket/input.csv',
                            type='file',
                        ),
                        SimpleNamespace(
                            name='local-source',
                            path='input.csv',
                            type='file',
                        ),
                    ],
                ),
                {'AWS_PROFILE': 'default'},
                id='s3-env-hint',
            ),
        ],
    )
    def test_provider_environment_rows_return_empty_when_auth_hints_exist(
        self,
        cfg: object,
        env: dict[str, str],
    ) -> None:
        """Provider rows should stay empty when explicit auth hints are present."""
        rows = readiness_mod.ReadinessReportBuilder.provider_environment_rows(
            cfg=cast(Any, cfg),
            env=env,
        )

        assert not rows

    def test_provider_environment_rows_skip_non_string_paths_and_warn_for_azure(
        self,
    ) -> None:
        """Test Azure warning branch and skipping connectors without string paths."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(name='ignored', path=None, type='file'),
                SimpleNamespace(
                    name='blob-source',
                    path='azure-blob://container/input.csv',
                    type='file',
                ),
            ],
        )

        rows = readiness_mod.ReadinessReportBuilder.provider_environment_rows(
            cfg=cast(Any, cfg),
            env={'AZURE_STORAGE_ACCOUNT_URL': 'https://account.blob.core.windows.net'},
        )

        assert rows == [
            {
                'connector': 'blob-source',
                'guidance': (
                    'Set AZURE_STORAGE_CREDENTIAL when the target is not public, '
                    'or use AZURE_STORAGE_CONNECTION_STRING for a fully explicit '
                    'configuration.'
                ),
                'missing_env': ['AZURE_STORAGE_CREDENTIAL'],
                'provider': 'azure-storage',
                'reason': (
                    'azure-blob access has no explicit Azure credential configured; '
                    'runtime access will only work for public resources or other '
                    'ambient authentication handled by the SDK call site.'
                ),
                'role': 'source',
                'scheme': 'azure-blob',
                'severity': 'warn',
            },
        ]

    def test_runtime_wrapper_helpers_delegate_to_extracted_helpers(
        self,
    ) -> None:
        """
        Thin readiness wrapper methods should preserve the extracted helper
        behavior.
        """
        builder = readiness_mod.ReadinessReportBuilder
        requirement = readiness_mod._RequirementSpec(('boto3',), 'boto3', 'storage')

        assert builder.aws_env_hint_present({'AWS_PROFILE': 'default'}) is True
        assert (
            builder.azure_authority_has_account_host(
                'azure-blob://container@account.blob.core.windows.net/data.csv',
            )
            is True
        )
        assert builder.connector_type('file') is readiness_mod.DataConnectorType.FILE
        assert builder.requirement_row(
            connector='out',
            detected_scheme='s3',
            reason='s3 storage path requires boto3',
            requirement=requirement,
            role='target',
        ) == {
            'connector': 'out',
            'detected_scheme': 's3',
            'extra': 'storage',
            'guidance': (
                'Install boto3 directly or install the ETLPlus "storage" extra. '
                'Required for "s3" storage paths.'
            ),
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'role': 'target',
        }

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

    def test_strict_config_report_wraps_config_checks_without_runtime_rows(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Strict report wrapper should force strict mode and omit runtime checks."""
        captured: dict[str, object] = {}

        def _config_checks(
            config_path: str,
            *,
            env: Mapping[str, str] | None,
            strict: bool = False,
            include_runtime_checks: bool = True,
        ) -> list[dict[str, object]]:
            captured.update(
                {
                    'config_path': config_path,
                    'env': env,
                    'strict': strict,
                    'include_runtime_checks': include_runtime_checks,
                },
            )
            return [{'name': 'config-structure', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'config_checks',
            _config_checks,
        )

        report = readiness_mod.ReadinessReportBuilder.strict_config_report(
            config_path='pipeline.yml',
            env={'MODE': 'test'},
        )

        assert report == {
            'checks': [{'name': 'config-structure', 'status': 'ok'}],
            'etlplus_version': readiness_mod.__version__,
            'status': 'ok',
        }
        assert captured == {
            'config_path': 'pipeline.yml',
            'env': {'MODE': 'test'},
            'strict': True,
            'include_runtime_checks': False,
        }

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

    @pytest.mark.parametrize(
        ('version', 'version_info', 'expected'),
        [
            pytest.param(
                '3.12.9',
                (3, 12, 9),
                {
                    'message': (
                        'Python 3.12.9 is outside the supported ETLPlus runtime '
                        'range (>=3.13,<3.15).'
                    ),
                    'name': 'python-version',
                    'status': 'error',
                    'version': '3.12.9',
                },
                id='unsupported',
            ),
            pytest.param(
                '3.13.12',
                (3, 13, 12),
                {
                    'message': (
                        'Python 3.13.12 is within the supported ETLPlus runtime range.'
                    ),
                    'name': 'python-version',
                    'status': 'ok',
                    'version': '3.13.12',
                },
                id='supported',
            ),
        ],
    )
    def test_supported_python_check_reports_expected_status(
        self,
        monkeypatch: pytest.MonkeyPatch,
        version: str,
        version_info: tuple[int, int, int],
        expected: dict[str, str],
    ) -> None:
        """Supported-Python readiness should reflect the active interpreter range."""
        monkeypatch.setattr(
            readiness_mod.ReadinessReportBuilder,
            'python_version',
            classmethod(lambda cls: version),
        )
        monkeypatch.setattr(
            readiness_mod,
            'sys',
            SimpleNamespace(version_info=version_info),
        )

        check = readiness_mod.ReadinessReportBuilder.supported_python_check()

        assert check == expected

    def test_token_reference_rows_collect_nested_config_paths(
        self,
    ) -> None:
        """Token references should preserve stable dotted and indexed paths."""
        rows = readiness_mod.ReadinessReportBuilder.token_reference_rows(
            {
                'profile': {'env': {'API_TOKEN': '${TOKEN_A}'}},
                'targets': [{'path': 's3://${TOKEN_B}/out.json'}],
                'jobs': [{'extract': {'source': '${TOKEN_A}'}}],
            },
        )

        assert rows == [
            {
                'name': 'TOKEN_A',
                'paths': [
                    'jobs[0].extract.source',
                    'profile.env.API_TOKEN',
                ],
            },
            {
                'name': 'TOKEN_B',
                'paths': ['targets[0].path'],
            },
        ]

    def test_token_reference_rows_cover_set_frozenset_and_scalar_fallthrough(
        self,
    ) -> None:
        """Token-reference walking should also cover unordered sets and scalars."""
        rows = readiness_mod.ReadinessReportBuilder.token_reference_rows(
            {
                'set_values': {'${SET_TOKEN}'},
                'frozen_values': frozenset({'${FROZEN_TOKEN}'}),
                'number': 1,
            },
        )

        assert rows == [
            {
                'name': 'FROZEN_TOKEN',
                'paths': ['frozen_values[0]'],
            },
            {
                'name': 'SET_TOKEN',
                'paths': ['set_values[0]'],
            },
        ]
