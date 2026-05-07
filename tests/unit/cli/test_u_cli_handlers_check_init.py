"""
:mod:`tests.unit.cli.test_u_cli_handlers_check_init` module.

Unit tests for config-check, init, and shared helper entry points in
:mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from etlplus import Config
from etlplus.cli._handlers import _lifecycle as lifecycle_mod
from etlplus.cli._handlers import check as check_mod
from etlplus.cli._handlers import init as init_mod

from .conftest import CaptureIo
from .conftest import assert_emit_json
from .pytest_cli_handlers_support import handlers
from .pytest_cli_handlers_support import patch_config_from_yaml

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='command_context')
def command_context_fixture() -> lifecycle_mod.CommandContext:
    """Return one command context for shared completion-helper tests."""
    return handlers._CommandContext(
        command='extract',
        event_format=None,
        run_id='run-123',
        started_at='2026-03-23T00:00:00Z',
        started_perf=0.0,
    )


# SECTION: TESTS ============================================================ #


class TestCheckHandler:
    """Unit tests for :func:`check_handler`."""

    def test_graph_branch_emits_dag_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that graph mode emits the ordered job graph summary.
        """
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'graph_summary',
            lambda _cfg: {
                'jobs': [{'depends_on': [], 'name': 'j1'}],
                'ordered_jobs': ['j1'],
                'status': 'ok',
            },
        )

        assert handlers.check_handler(config='cfg.yml', graph=True) == 0
        assert_emit_json(
            capture_io,
            {
                'jobs': [{'depends_on': [], 'name': 'j1'}],
                'ordered_jobs': ['j1'],
                'status': 'ok',
            },
            pretty=True,
        )

    def test_graph_branch_returns_error_payload_for_invalid_graph(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that graph mode surfaces graph validation errors as JSON.
        """
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'graph_summary',
            lambda _cfg: (_ for _ in ()).throw(ValueError('Dependency cycle detected')),
        )

        assert handlers.check_handler(config='cfg.yml', graph=True) == 1
        assert_emit_json(
            capture_io,
            {
                'message': 'Dependency cycle detected',
                'status': 'error',
            },
            pretty=True,
        )

    def test_passes_substitute_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`check_handler` forwards the substitute flag to config
        loader.
        """
        load_calls: list[tuple[str, bool]] = []
        patch_config_from_yaml(
            monkeypatch,
            dummy_cfg,
            calls=load_calls,
        )
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'pipelines': ['p1']},
        )
        assert handlers.check_handler(config='cfg.yml', substitute=True) == 0
        assert load_calls == [('cfg.yml', True)]
        assert_emit_json(capture_io, {'pipelines': ['p1']}, pretty=True)

    def test_prints_sections(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`check_handler` prints requested sections."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'targets': ['t1']},
        )
        assert handlers.check_handler(config='cfg.yml') == 0
        assert_emit_json(capture_io, {'targets': ['t1']}, pretty=True)

    def test_readiness_branch_emits_report_and_returns_nonzero_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that readiness mode emits the runtime report and exit code."""
        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'build',
            lambda config_path: {
                'checks': [],
                'status': 'error',
            },
        )

        assert handlers.check_handler(config='cfg.yml', readiness=True) == 1
        assert_emit_json(
            capture_io,
            {'checks': [], 'status': 'error'},
            pretty=True,
        )

    def test_readiness_branch_returns_zero_on_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Readiness warnings should emit the report without failing the CLI."""
        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'build',
            lambda config_path: {
                'checks': [],
                'status': 'warn',
            },
        )

        assert handlers.check_handler(config='cfg.yml', readiness=True) == 0
        assert_emit_json(
            capture_io,
            {'checks': [], 'status': 'warn'},
            pretty=True,
        )

    def test_requires_config_when_not_in_readiness_mode(self) -> None:
        """
        Test that non-readiness check mode requires a config path.
        """
        with pytest.raises(
            ValueError,
            match='config is required unless readiness-only mode is used',
        ):
            handlers.check_handler(config=None, readiness=False)

    def test_strict_branch_emits_report_and_skips_config_load_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that strict check mode emits the strict report before config
        load.
        """
        config_loaded = {'value': False}

        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'strict_config_report',
            lambda config_path: {
                'checks': [{'name': 'config-structure', 'status': 'error'}],
                'status': 'error',
            },
        )
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda *_args, **_kwargs: config_loaded.__setitem__('value', True),
        )

        assert handlers.check_handler(config='cfg.yml', strict=True) == 1
        assert config_loaded['value'] is False
        assert_emit_json(
            capture_io,
            {
                'checks': [{'name': 'config-structure', 'status': 'error'}],
                'status': 'error',
            },
            pretty=True,
        )

    def test_strict_branch_falls_through_to_config_load_when_report_is_ok(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that strict check mode continues into config loading on success.
        """
        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'strict_config_report',
            lambda config_path: {
                'checks': [{'name': 'config-structure', 'status': 'ok'}],
                'status': 'ok',
            },
        )
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'jobs': ['j1']},
        )

        assert handlers.check_handler(config='cfg.yml', strict=True) == 0
        assert_emit_json(capture_io, {'jobs': ['j1']}, pretty=True)

    def test_summary_branch_uses_pipeline_summary_with_requested_pretty_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that summary mode preserves the caller's pretty setting."""
        patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'pipeline_summary',
            lambda _cfg: {'name': 'p1', 'jobs': ['j1']},
        )

        assert (
            handlers.check_handler(
                config='cfg.yml',
                summary=True,
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {'name': 'p1', 'jobs': ['j1']},
            pretty=False,
        )


class TestInitHandler:
    """Unit tests for :func:`init_handler`."""

    def test_overwrites_existing_scaffold_when_force_is_true(
        self,
        tmp_path: Path,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that init handler overwrites scaffold files when force is enabled.
        """
        project_dir = tmp_path / 'starter'
        data_dir = project_dir / 'data'
        data_dir.mkdir(parents=True)
        (project_dir / 'pipeline.yml').write_text('name: old\n', encoding='utf-8')
        (data_dir / 'customers.csv').write_text('old,data\n', encoding='utf-8')

        assert handlers.init_handler(directory=str(project_dir), force=True) == 0
        assert 'Starter Pipeline' in (project_dir / 'pipeline.yml').read_text(
            encoding='utf-8',
        )
        assert 'Alice Example' in (data_dir / 'customers.csv').read_text(
            encoding='utf-8',
        )
        assert_emit_json(
            capture_io,
            cast(dict[str, Any], capture_io['emit_json'][0][0][0]),
            pretty=True,
        )

    def test_refuses_to_overwrite_existing_scaffold_without_force(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that init handler requires force before overwriting scaffold files.
        """
        project_dir = tmp_path / 'starter'
        project_dir.mkdir()
        (project_dir / 'pipeline.yml').write_text('name: existing\n', encoding='utf-8')

        with pytest.raises(ValueError, match='Scaffold file already exists'):
            handlers.init_handler(directory=str(project_dir))

    def test_rejects_target_path_when_directory_argument_is_a_file(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that init handler rejects a target path that already points to a file.
        """
        file_path = tmp_path / 'starter'
        file_path.write_text('not-a-directory\n', encoding='utf-8')

        with pytest.raises(ValueError, match='Init target must be a directory'):
            handlers.init_handler(directory=str(file_path))


class TestInitHandlerInternalHelpers:
    """Unit tests for internal helper functions in :mod:`etlplus.cli._handlers.init`."""

    def test_next_steps_omits_cd_when_root_matches_current_working_directory(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that next-step suggestions omit cd when already in the target
        root.
        """
        monkeypatch.chdir(tmp_path)

        steps = init_mod._next_steps(tmp_path)

        assert steps == [
            'etlplus check --config pipeline.yml --jobs',
            'etlplus check --readiness --config pipeline.yml --strict',
            'etlplus run --config pipeline.yml --job file_to_file_customers',
        ]

    def test_write_text_file_rejects_directory_targets(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that direct scaffold file writes reject directory targets.
        """
        target_dir = tmp_path / 'pipeline.yml'
        target_dir.mkdir()

        with pytest.raises(
            ValueError,
            match='Cannot write scaffold file over directory',
        ):
            init_mod._write_text_file(
                target_dir,
                'payload\n',
                force=False,
            )


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`handlers`."""

    def test_check_sections_all(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` includes all requested sections."""
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_check_sections_default(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` defaults to jobs when no flags are
        set.
        """
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        assert 'jobs' in result

    def test_check_sections_jobs_and_mapping_transforms(
        self,
        dummy_cfg: Config,
    ) -> None:
        """Test that jobs flag plus mapping-style transforms extraction."""
        cfg = SimpleNamespace(
            name=dummy_cfg.name,
            version=dummy_cfg.version,
            sources=dummy_cfg.sources,
            targets=dummy_cfg.targets,
            jobs=dummy_cfg.jobs,
            transforms={
                'trim': {'field': 'name'},
                'dedupe': {'on': 'id'},
            },
        )

        result = handlers._check_sections(
            cast(Config, cfg),
            jobs=True,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=True,
        )
        assert result['jobs'] == ['j1']
        assert result['transforms'] == ['trim', 'dedupe']

    def test_complete_output_json_file_emits_for_stdout_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        command_context: lifecycle_mod.CommandContext,
    ) -> None:
        """JSON-file completion should emit JSON when no file target is supplied."""
        emitted: dict[str, object] = {}
        monkeypatch.setattr(
            lifecycle_mod,
            'complete_command',
            lambda _context, **_fields: None,
        )
        monkeypatch.setattr(
            handlers._output,
            'emit_json_payload',
            lambda payload, *, pretty: (
                emitted.update(
                    {'payload': payload, 'pretty': pretty},
                )
                or 7
            ),
        )

        assert (
            handlers._complete_output(
                command_context,
                {'ok': True},
                mode='json_file',
                output_path=None,
                pretty=False,
                success_message='Saved to',
            )
            == 7
        )
        assert emitted == {'payload': {'ok': True}, 'pretty': False}

    @pytest.mark.parametrize(
        ('kwargs', 'expected_message'),
        [
            pytest.param(
                {'mode': 'file'},
                "'file' completion requires an output path",
                id='file-missing-target',
            ),
            pytest.param(
                {'mode': 'file', 'output_path': '-'},
                "'file' completion requires an output path",
                id='file-stdout-target',
            ),
            pytest.param(
                {'mode': 'or_write', 'output_path': 'out.json'},
                "'or_write' completion requires a success message",
                id='or-write-missing-message',
            ),
            pytest.param(
                {'mode': 'json_file', 'output_path': 'out.json'},
                "'json_file' completion requires a success message",
                id='json-file-missing-message',
            ),
        ],
    )
    def test_complete_output_rejects_incomplete_file_modes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        command_context: lifecycle_mod.CommandContext,
        kwargs: dict[str, object],
        expected_message: str,
    ) -> None:
        """File-writing completion modes should fail before writing bad values."""
        monkeypatch.setattr(
            lifecycle_mod,
            'complete_command',
            lambda _context, **_fields: None,
        )

        with pytest.raises(ValueError, match=expected_message):
            handlers._complete_output(command_context, {'ok': True}, **kwargs)

    def test_complete_output_rejects_unknown_modes(
        self,
        command_context: lifecycle_mod.CommandContext,
    ) -> None:
        """
        Test that unknown completion modes fail fast with an assertion.
        """
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(
                lifecycle_mod,
                'complete_command',
                lambda _context, **_fields: None,
            )
            with pytest.raises(AssertionError, match='Unsupported completion mode'):
                handlers._complete_output(
                    command_context,
                    {'ok': True},
                    mode=cast(Any, 'unsupported'),
                )

    def test_failure_boundary_invokes_on_error_callback(self) -> None:
        """
        Test that the failure boundary calls its optional error callback.
        """
        captured: dict[str, object] = {}

        def on_error(exc: Exception) -> None:
            captured['exc'] = exc

        def fake_fail_command(
            context: object,
            exc: Exception,
            **fields: object,
        ) -> None:
            captured['context'] = context
            captured['failed'] = (exc, fields)

        context = handlers._CommandContext(
            command='extract',
            event_format=None,
            run_id='run-123',
            started_at='2026-03-23T00:00:00Z',
            started_perf=0.0,
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(lifecycle_mod, 'fail_command', fake_fail_command)
            with pytest.raises(RuntimeError, match='boom'):
                with handlers._failure_boundary(
                    context,
                    on_error=on_error,
                    step='extract',
                ):
                    raise RuntimeError('boom')

        assert isinstance(captured['exc'], RuntimeError)
        failed_exc, failed_fields = cast(
            tuple[Exception, dict[str, object]],
            captured['failed'],
        )
        assert isinstance(failed_exc, RuntimeError)
        assert failed_fields == {'step': 'extract'}

    def test_pipeline_summary(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        summary = handlers._pipeline_summary(dummy_cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_summary_collect_table_specs_merges_config_and_spec(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that config and standalone spec entries are merged."""
        spec_path = tmp_path / 'table.json'
        spec_path.write_text('{}', encoding='utf-8')
        monkeypatch.setattr(
            handlers._summary,
            'load_table_spec',
            lambda _path: {'table': 'from_spec'},
        )
        monkeypatch.setattr(
            handlers._summary.Config,
            'from_yaml',
            lambda _path, substitute: SimpleNamespace(
                table_schemas=[{'table': 'from_config'}],
            ),
        )

        specs = handlers._summary.collect_table_specs(
            config_path='pipeline.yml',
            spec_path=str(spec_path),
        )
        assert specs == [
            {'table': 'from_spec'},
            {'table': 'from_config'},
        ]


class TestScheduleHandler:
    """Unit tests for :func:`schedule_handler`."""

    def test_emits_schedule_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should emit the configured schedule metadata."""
        cfg = Config.from_dict(
            {
                'name': 'Schedule Test Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [
                    {
                        'name': 'nightly_all',
                        'cron': '0 2 * * *',
                        'timezone': 'UTC',
                        'target': {'run_all': True},
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'paused': True,
                        'target': {'job': 'job-a'},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)

        assert handlers.schedule_handler(config='cfg.yml') == 0
        assert_emit_json(
            capture_io,
            {
                'name': 'Schedule Test Pipeline',
                'schedule_count': 2,
                'schedules': [
                    {
                        'name': 'nightly_all',
                        'cron': '0 2 * * *',
                        'paused': False,
                        'target': {'run_all': True},
                        'timezone': 'UTC',
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'paused': True,
                        'target': {'job': 'job-a'},
                    },
                ],
            },
            pretty=True,
        )

    @pytest.mark.parametrize('conflicting_dir', ['data', 'temp'])
    def test_rejects_existing_file_where_scaffold_directory_is_required(
        self,
        tmp_path: Path,
        conflicting_dir: str,
    ) -> None:
        """
        Test that init handler rejects files where scaffold directories are needed.
        """
        project_dir = tmp_path / 'starter'
        project_dir.mkdir()
        (project_dir / conflicting_dir).write_text('conflict\n', encoding='utf-8')

        with pytest.raises(
            ValueError,
            match='Init target requires a directory but found a file',
        ):
            handlers.init_handler(directory=str(project_dir))

    def test_scaffolds_starter_files(
        self,
        tmp_path: Path,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that init handler creates starter files and emits a JSON payload.
        """
        project_dir = tmp_path / 'starter'

        assert handlers.init_handler(directory=str(project_dir)) == 0
        assert (project_dir / 'pipeline.yml').is_file()
        assert (project_dir / 'data' / 'customers.csv').is_file()
        payload = cast(dict[str, Any], capture_io['emit_json'][0][0][0])
        assert payload['status'] == 'ok'
        assert payload['job'] == 'file_to_file_customers'
