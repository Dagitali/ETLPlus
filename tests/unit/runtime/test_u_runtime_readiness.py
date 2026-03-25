"""
:mod:`tests.unit.test_u_runtime_readiness` module.

Unit tests for :mod:`etlplus.runtime.readiness`.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime.readiness as readiness_module

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilder:
    """Unit tests for :class:`ReadinessReportBuilder`."""

    def test_build_matches_wrapper_runtime_only(self) -> None:
        """Test that the class builder matches the function wrapper."""
        expected = readiness_module.build_readiness_report(env={})
        actual = readiness_module.ReadinessReportBuilder.build(env={})

        assert actual == expected

    def test_build_wraps_config_check_exceptions(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that build converts config-check exceptions into error rows."""
        monkeypatch.setattr(
            readiness_module,
            '_config_checks',
            lambda _config_path, env=None: (_ for _ in ()).throw(TypeError('boom')),
        )

        report = readiness_module.ReadinessReportBuilder.build(
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

    def test_config_checks_returns_missing_file_error(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that config checks return a config-file error for missing paths."""
        missing_path = tmp_path / 'missing.yml'

        checks = readiness_module.ReadinessReportBuilder.config_checks(
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

    def test_config_checks_unresolved_substitutions_skip_connector_checks(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test that unresolved substitutions short-circuit connector checks."""
        config_path = tmp_path / 'pipeline.yml'
        config_path.write_text('name: pipeline\n', encoding='utf-8')

        monkeypatch.setattr(
            readiness_module,
            '_load_raw_config',
            lambda _path: {'profile': {'env': {}}, 'vars': {'token': 'value'}},
        )

        def _config_from_dict(raw: dict[str, object]) -> object:
            profile = raw.get('profile')
            profile_env = profile.get('env', {}) if isinstance(profile, dict) else {}
            return SimpleNamespace(
                profile=SimpleNamespace(env=profile_env),
                vars=raw.get('vars', {}),
            )

        monkeypatch.setattr(readiness_module.Config, 'from_dict', _config_from_dict)
        monkeypatch.setattr(
            readiness_module,
            'deep_substitute',
            lambda raw, _vars, _env: {'token': '${MISSING_TOKEN}'},
        )

        connector_checks_called = {'value': False}

        def _connector_readiness_checks(_cfg: object) -> list[dict[str, object]]:
            connector_checks_called['value'] = True
            return [{'name': 'connector-readiness', 'status': 'ok'}]

        monkeypatch.setattr(
            readiness_module,
            '_connector_readiness_checks',
            _connector_readiness_checks,
        )

        checks = readiness_module.ReadinessReportBuilder.config_checks(
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
                'name': 'config-substitution',
                'status': 'error',
                'unresolved_tokens': ['MISSING_TOKEN'],
            },
        ]

    def test_missing_requirement_rows_respects_package_available_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based dependency checks still honor wrapper patches."""
        monkeypatch.setattr(
            readiness_module,
            '_package_available',
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

        rows = readiness_module.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert rows == [
            {
                'connector': 's3-source',
                'extra': 'storage',
                'missing_package': 'boto3',
                'reason': 's3 storage path requires boto3',
                'role': 'source',
            },
        ]
