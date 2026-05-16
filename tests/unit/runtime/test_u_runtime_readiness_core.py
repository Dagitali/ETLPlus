"""
:mod:`tests.unit.runtime.test_u_runtime_readiness_core` module.

Core unit tests for :mod:`etlplus.runtime.readiness._builder`.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace

import pytest

import etlplus.runtime.readiness._base as readiness_base_mod
import etlplus.runtime.readiness._builder as readiness_builder_mod
import etlplus.runtime.readiness._connectors as readiness_connectors_mod
import etlplus.runtime.readiness._providers as readiness_providers_mod
import etlplus.runtime.readiness._strict as readiness_strict_mod

from .pytest_runtime_readiness import build_runtime_cfg as _cfg
from .pytest_runtime_readiness import (
    patch_config_resolution as _patch_config_resolution,
)
from .pytest_runtime_readiness import patch_file_read as _patch_file_read
from .pytest_runtime_readiness import write_pipeline_config

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilderCore:
    """Core unit tests for :class:`ReadinessReportBuilder`."""

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
            readiness_builder_mod.ReadinessReportBuilder,
            'config_checks',
            _config_checks,
        )

        report = readiness_builder_mod.ReadinessReportBuilder.build(
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

    def test_build_runtime_only_emits_python_and_skipped_config_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that runtime-only builds emit the stable non-config readiness rows.
        """
        monkeypatch.setattr(
            readiness_builder_mod.ReadinessReportBuilder,
            'supported_python_check',
            lambda: {'name': 'python-version', 'status': 'ok'},
        )
        monkeypatch.setattr(
            readiness_builder_mod.ReadinessReportBuilder,
            'python_version',
            classmethod(lambda cls: '3.13.12'),
        )

        report = readiness_builder_mod.ReadinessReportBuilder.build(env={})

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
            'etlplus_version': readiness_builder_mod._ETLPLUS_VERSION,
            'python_version': '3.13.12',
            'status': 'ok',
        }

    def test_build_wraps_config_check_exceptions(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that build converts config-check exceptions into error rows."""
        monkeypatch.setattr(
            readiness_builder_mod.ReadinessReportBuilder,
            'config_checks',
            lambda _config_path, env=None: (_ for _ in ()).throw(TypeError('boom')),
        )

        report = readiness_builder_mod.ReadinessReportBuilder.build(
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
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_connector_storage_scheme(
                '',
            )
            is None
        )
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_connector_storage_scheme(
                's3',
            )
            == 's3'
        )
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_connector_storage_scheme(
                'not-a-real-scheme',
            )
            is None
        )

    def test_coerce_storage_scheme_handles_missing_and_unknown_schemes(
        self,
    ) -> None:
        """Test storage-scheme coercion for local, blank, known, and unknown."""
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_storage_scheme(
                'local/path.csv',
            )
            is None
        )
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_storage_scheme(
                '://missing',
            )
            is None
        )
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_storage_scheme(
                's3://bucket/input.csv',
            )
            == 's3'
        )
        assert (
            readiness_base_mod.ReadinessSupportPolicy.coerce_storage_scheme(
                'custom://bucket/input.csv',
            )
            == 'custom'
        )

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

        tokens = readiness_base_mod.TokenReferenceCollector.collect_names(
            value,
        )

        assert tokens == {
            'FROZEN_TOKEN',
            'LIST_TOKEN',
            'SET_TOKEN',
            'TUPLE_TOKEN',
        }

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
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: _connector_readiness_checks(_args[0]),
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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

    def test_config_checks_schedule_errors_skip_runtime_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Schedule validation errors should fail readiness before runtime checks."""
        config_path = write_pipeline_config(tmp_path)
        resolved_cfg = _cfg()
        resolved_cfg.jobs = [SimpleNamespace(name='job-a')]
        resolved_cfg.schedules = [
            SimpleNamespace(
                name='nightly_all',
                cron='*/15 * * * *',
                interval=None,
                paused=False,
                target=SimpleNamespace(job=None, run_all=True),
                timezone='UTC',
                backfill=None,
            ),
        ]
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
            resolved_cfg=resolved_cfg,
        )
        runtime_called = {'value': False}

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            runtime_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: _connector_readiness_checks(_args[0]),
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
        )

        assert runtime_called['value'] is False
        schedule_check = next(
            check for check in checks if check['name'] == 'schedule-config'
        )
        assert schedule_check['status'] == 'error'
        assert any(
            issue['issue']
            == (
                'cron helper emission currently supports only '
                'single values or "*" fields'
            )
            for issue in schedule_check['issues']
        )

    def test_config_checks_schedule_validation_accepts_supported_schedule_shape(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Supported schedule shapes should continue into runtime readiness checks."""
        config_path = write_pipeline_config(tmp_path)
        resolved_cfg = _cfg()
        resolved_cfg.jobs = [SimpleNamespace(name='job-a')]
        resolved_cfg.schedules = [
            SimpleNamespace(
                name='nightly_all',
                cron='0 2 * * *',
                interval=None,
                paused=False,
                target=SimpleNamespace(job='job-a', run_all=False),
                timezone='UTC',
                backfill=None,
            ),
        ]
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
            resolved_cfg=resolved_cfg,
        )
        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: [{'name': 'connector-readiness', 'status': 'ok'}],
        )
        monkeypatch.setattr(
            readiness_providers_mod.ProviderEnvironmentPolicy,
            'environment_checks',
            lambda **_kwargs: [{'name': 'provider-environment', 'status': 'ok'}],
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env={},
        )

        assert not any(check['name'] == 'schedule-config' for check in checks)
        assert any(check['name'] == 'connector-readiness' for check in checks)
        assert any(check['name'] == 'provider-environment' for check in checks)

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
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: _connector_readiness_checks(_args[0]),
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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
            readiness_strict_mod.StrictConfigValidator,
            'config_issue_rows',
            lambda **_kwargs: [],
        )
        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: [{'name': 'connector-readiness', 'status': 'ok'}],
        )
        monkeypatch.setattr(
            readiness_providers_mod.ProviderEnvironmentPolicy,
            'environment_checks',
            lambda **_kwargs: [{'name': 'provider-environment', 'status': 'ok'}],
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: [{'name': 'connector-readiness', 'status': 'ok'}],
        )
        monkeypatch.setattr(
            readiness_providers_mod.ProviderEnvironmentPolicy,
            'environment_checks',
            lambda **_kwargs: [{'name': 'provider-environment', 'status': 'ok'}],
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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
            readiness_builder_mod.ReadinessReportBuilder,
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

        monkeypatch.setattr(
            readiness_builder_mod.Config,
            'from_dict',
            _config_from_dict,
        )
        monkeypatch.setattr(
            readiness_base_mod.SubstitutionResolver,
            'deep',
            lambda _resolver, raw: {'token': '${MISSING_TOKEN}'},
        )

        connector_checks_called = {'value': False}

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            connector_checks_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: _connector_readiness_checks(_args[0]),
        )

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
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

    @pytest.mark.parametrize(
        ('issue', 'api_reference', 'expected'),
        [
            pytest.param(
                'missing path',
                None,
                'Set "path" to a local path or storage URI for this file connector.',
                id='missing-path',
            ),
            pytest.param(
                'missing url or api reference',
                None,
                'Set "url" to a reachable endpoint or "api" to a configured '
                'top-level API name.',
                id='missing-url-or-api',
            ),
            pytest.param(
                'missing connection_string',
                None,
                'Set "connection_string" to a database DSN or SQLAlchemy-style URL.',
                id='missing-connection-string',
            ),
            pytest.param(
                'unknown api reference: missing-api',
                'missing-api',
                'Define "missing-api" under top-level "apis" or update the '
                'connector "api" reference.',
                id='unknown-api-with-reference-name',
            ),
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
            readiness_base_mod.ReadinessSupportPolicy.connector_gap_guidance(
                api_reference=api_reference,
                issue=issue,
            )
            == expected
        )

    def test_iter_connectors_yields_sources_then_targets(
        self,
    ) -> None:
        """Connector iteration should preserve source-then-target ordering."""
        cfg = _cfg(
            sources=[SimpleNamespace(name='src-a'), SimpleNamespace(name='src-b')],
            targets=[SimpleNamespace(name='dst-a')],
        )

        rows = list(readiness_base_mod.ReadinessSupportPolicy.iter_connectors(cfg))

        assert [(role, connector.name) for role, connector in rows] == [
            ('source', 'src-a'),
            ('source', 'src-b'),
            ('target', 'dst-a'),
        ]

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
                readiness_builder_mod.ReadinessReportBuilder.load_raw_config(
                    'pipeline.yml',
                )
            return

        assert (
            readiness_builder_mod.ReadinessReportBuilder.load_raw_config(
                'pipeline.yml',
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('kwargs', 'expected'),
        [
            pytest.param(
                {'detected_format': 'nc', 'package': 'xarray/netCDF4', 'extra': 'file'},
                'Install xarray plus one of netCDF4 or h5netcdf, or install the '
                'ETLPlus "file" extra.',
                id='netcdf',
            ),
            pytest.param(
                {'detected_format': 'csv', 'package': 'pyarrow', 'extra': 'file'},
                'Install pyarrow directly or install the ETLPlus "file" extra. '
                'Required for "csv" file format.',
                id='format',
            ),
            pytest.param(
                {'detected_scheme': 's3', 'package': 'boto3', 'extra': 'storage'},
                'Install boto3 directly or install the ETLPlus "storage" extra. '
                'Required for "s3" storage paths.',
                id='scheme',
            ),
        ],
    )
    def test_missing_requirement_guidance_covers_contextual_variants(
        self,
        kwargs: dict[str, str],
        expected: str,
    ) -> None:
        """Missing-dependency guidance should reflect format and scheme context."""
        assert (
            readiness_base_mod.ReadinessSupportPolicy.missing_requirement_guidance(
                **kwargs,
            )
            == expected
        )

    def test_overall_status_warn_when_no_errors_exist(self) -> None:
        """Test aggregate status when warnings exist without errors."""
        checks = [
            {'status': 'ok'},
            {'status': 'warn'},
        ]

        assert (
            readiness_builder_mod.ReadinessReportBuilder.overall_status(checks)
            == 'warn'
        )

    def test_package_available_handles_availability_helper_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test package availability failures when helper lookup raises."""
        monkeypatch.setattr(
            readiness_base_mod,
            'module_available',
            lambda _module_name: (_ for _ in ()).throw(ValueError('boom')),
        )

        assert (
            readiness_builder_mod.ReadinessReportBuilder.package_available('broken')
            is False
        )

    def test_config_checks_delegate_to_provider_environment_policy(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Config checks should forward provider seams into the policy call."""
        captured: dict[str, object] = {}

        config_path = write_pipeline_config(tmp_path)
        resolved_cfg = _cfg()
        _patch_config_resolution(
            monkeypatch,
            raw={'name': 'pipeline'},
            resolved_cfg=resolved_cfg,
        )
        monkeypatch.setattr(
            readiness_connectors_mod.ConnectorReadinessPolicy,
            'readiness_checks',
            lambda *_args, **_kwargs: [],
        )

        def _environment_checks(
            *,
            cfg: object,
            env: Mapping[str, str],
            make_check: object,
            provider_environment_rows_fn: object,
        ) -> list[dict[str, object]]:
            captured.update(
                {
                    'cfg': cfg,
                    'env': env,
                    'make_check': make_check,
                    'provider_environment_rows_fn': provider_environment_rows_fn,
                },
            )
            return [{'name': 'provider-environment', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_providers_mod.ProviderEnvironmentPolicy,
            'environment_checks',
            _environment_checks,
        )
        env = {'AWS_PROFILE': 'dev'}

        checks = readiness_builder_mod.ReadinessReportBuilder.config_checks(
            str(config_path),
            env=env,
        )

        assert checks[-1:] == [{'name': 'provider-environment', 'status': 'ok'}]
        assert captured == {
            'cfg': resolved_cfg,
            'env': env,
            'make_check': readiness_builder_mod.ReadinessReportBuilder.make_check,
            'provider_environment_rows_fn': (
                readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows
            ),
        }

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
            readiness_builder_mod.ReadinessReportBuilder,
            'config_checks',
            _config_checks,
        )

        report = readiness_builder_mod.ReadinessReportBuilder.strict_config_report(
            config_path='pipeline.yml',
            env={'MODE': 'test'},
        )

        assert report == {
            'checks': [{'name': 'config-structure', 'status': 'ok'}],
            'etlplus_version': readiness_builder_mod._ETLPLUS_VERSION,
            'status': 'ok',
        }
        assert captured == {
            'config_path': 'pipeline.yml',
            'env': {'MODE': 'test'},
            'strict': True,
            'include_runtime_checks': False,
        }

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
            readiness_builder_mod.ReadinessReportBuilder,
            'python_version',
            classmethod(lambda cls: version),
        )
        monkeypatch.setattr(
            readiness_base_mod,
            'sys',
            SimpleNamespace(version_info=version_info),
        )

        check = readiness_builder_mod.ReadinessReportBuilder.supported_python_check()

        assert check == expected

    def test_token_reference_rows_collect_nested_config_paths(
        self,
    ) -> None:
        """Token references should preserve stable dotted and indexed paths."""
        rows = readiness_base_mod.TokenReferenceCollector.collect_rows(
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
        rows = readiness_base_mod.TokenReferenceCollector.collect_rows(
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
