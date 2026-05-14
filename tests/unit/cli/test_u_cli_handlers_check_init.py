"""
:mod:`tests.unit.cli.test_u_cli_handlers_check_init` module.

Unit tests for config-check, init, and shared helper entry points in
:mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

import json
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
from etlplus.cli._handlers import schedule as schedule_mod

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
            lambda config_path, strict=False: {
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
            lambda config_path, strict=False: {
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

    def test_complete_output_json_file_returns_zero_after_successful_write(
        self,
        monkeypatch: pytest.MonkeyPatch,
        command_context: lifecycle_mod.CommandContext,
    ) -> None:
        """JSON-file completion should return success when file output is written."""
        writes: dict[str, object] = {}
        monkeypatch.setattr(
            lifecycle_mod,
            'complete_command',
            lambda _context, **_fields: None,
        )
        monkeypatch.setattr(
            handlers._output,
            'write_json_output',
            lambda payload, output_path, *, success_message: (
                writes.update(
                    {
                        'payload': payload,
                        'output_path': output_path,
                        'success_message': success_message,
                    },
                )
                or True
            ),
        )

        assert (
            handlers._complete_output(
                command_context,
                {'ok': True},
                mode='json_file',
                output_path='out.json',
                pretty=False,
                success_message='Saved to',
            )
            == 0
        )
        assert writes == {
            'payload': {'ok': True},
            'output_path': 'out.json',
            'success_message': 'Saved to',
        }

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
                {'mode': 'file', 'output_path': 'out.json'},
                "'file' completion requires a success message",
                id='file-missing-message',
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

    @pytest.mark.parametrize(
        ('cron', 'match'),
        [
            pytest.param(
                '0 2 * *',
                'supports exactly five cron fields',
                id='invalid-field-count',
            ),
            pytest.param(
                '*/5 2 * * *',
                'supports only single values or "\\*" fields',
                id='unsupported-token',
            ),
            pytest.param(
                '0 2 * * 8',
                'Cron weekday must be 0-7',
                id='invalid-weekday',
            ),
            pytest.param(
                'x 2 * * *',
                'Cron minute must be one integer value or "\\*"',
                id='invalid-minute',
            ),
            pytest.param(
                '0 x * * *',
                'Cron hour must be one integer value or "\\*"',
                id='invalid-hour',
            ),
            pytest.param(
                '0 2 x * *',
                'Cron day must be one integer value or "\\*"',
                id='invalid-day',
            ),
            pytest.param(
                '0 2 * x *',
                'Cron month must be one integer value or "\\*"',
                id='invalid-month',
            ),
        ],
    )
    def test_cron_to_on_calendar_rejects_unsupported_inputs(
        self,
        cron: str,
        match: str,
    ) -> None:
        """Schedule cron helper should reject unsupported cron expressions."""
        with pytest.raises(ValueError, match=match):
            schedule_mod._cron_to_on_calendar(cron)

    def test_cron_to_on_calendar_supports_weekday_prefixes(self) -> None:
        """Schedule cron helper should map weekday numbers into systemd names."""
        assert schedule_mod._cron_to_on_calendar('5 6 * * 1') == ('Mon *-*-* 06:05:00')

    def test_emits_crontab_helper_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should emit one crontab helper payload."""
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
                        'target': {'run_all': True},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)
        config_path = Path('cfg.yml').resolve()

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                emit='crontab',
                schedule_name='nightly_all',
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'format': 'crontab',
                'schedule': 'nightly_all',
                'snippet': (
                    f'0 2 * * * cd {config_path.parent} '
                    f'&& etlplus run --config {config_path} --all'
                ),
            },
            pretty=True,
        )

    def test_emits_helper_payload_for_later_matching_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule helper selection should continue past earlier non-matches."""
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
                        'target': {'run_all': True},
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'target': {'job': 'job-a'},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)
        config_path = Path('cfg.yml').resolve()

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                emit='crontab',
                schedule_name='nightly_all',
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'format': 'crontab',
                'schedule': 'nightly_all',
                'snippet': (
                    f'0 2 * * * cd {config_path.parent} '
                    f'&& etlplus run --config {config_path} --all'
                ),
            },
            pretty=True,
        )

    def test_emits_helper_payload_for_second_matching_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule resolution should keep scanning until a later name matches."""
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
                        'target': {'run_all': True},
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'target': {'job': 'job-a'},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)
        config_path = Path('cfg.yml').resolve()
        service_name = 'etlplus-customers-every-30m.service'
        timer_name = 'etlplus-customers-every-30m.timer'

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                emit='systemd',
                schedule_name='customers_every_30m',
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'format': 'systemd',
                'schedule': 'customers_every_30m',
                'service_name': service_name,
                'timer_name': timer_name,
                'service': (
                    '[Unit]\n'
                    'Description=ETLPlus schedule customers_every_30m\n\n'
                    '[Service]\n'
                    'Type=oneshot\n'
                    f'WorkingDirectory={config_path.parent}\n'
                    f'ExecStart=etlplus run --config {config_path} --job job-a\n'
                ),
                'timer': (
                    '[Unit]\n'
                    'Description=Run ETLPlus schedule customers_every_30m\n\n'
                    '[Timer]\n'
                    'OnUnitActiveSec=30m\n'
                    'Persistent=true\n'
                    f'Unit={service_name}\n\n'
                    '[Install]\n'
                    'WantedBy=timers.target\n'
                ),
            },
            pretty=True,
        )

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

    def test_emits_schedule_summary_with_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should optionally include persisted scheduler state."""
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
                        'target': {'run_all': True},
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'target': {'job': 'job-a'},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)
        state_dir = Path('state-dir').resolve()
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / 'scheduler-state.json').write_text(
            json.dumps(
                {
                    'schedules': {
                        'nightly_all': {
                            'last_attempted_at': '2026-05-12T02:00:00+00:00',
                            'last_completed_at': '2026-05-12T02:00:00+00:00',
                            'last_run_id': 'run-1',
                            'last_status': 'ok',
                        },
                    },
                },
            ),
            encoding='utf-8',
        )

        assert handlers.schedule_handler(config='cfg.yml', show_state=True) == 0
        assert_emit_json(
            capture_io,
            {
                'name': 'Schedule Test Pipeline',
                'schedule_count': 2,
                'schedules': [
                    {
                        'cron': '0 2 * * *',
                        'name': 'nightly_all',
                        'paused': False,
                        'state': {
                            'last_attempted_at': '2026-05-12T02:00:00+00:00',
                            'last_completed_at': '2026-05-12T02:00:00+00:00',
                            'last_run_id': 'run-1',
                            'last_status': 'ok',
                        },
                        'target': {'run_all': True},
                    },
                    {
                        'interval': {'minutes': 30},
                        'name': 'customers_every_30m',
                        'paused': False,
                        'state': {},
                        'target': {'job': 'job-a'},
                    },
                ],
                'state_dir': str(state_dir),
            },
            pretty=True,
        )

    def test_emits_systemd_helper_payload_for_interval_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should emit one systemd helper payload."""
        cfg = Config.from_dict(
            {
                'name': 'Schedule Test Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [
                    {
                        'name': 'customers_every_30m',
                        'interval': {'minutes': 30},
                        'target': {'job': 'job-a'},
                    },
                ],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)
        config_path = Path('cfg.yml').resolve()
        service_name = 'etlplus-customers-every-30m.service'
        timer_name = 'etlplus-customers-every-30m.timer'

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                emit='systemd',
                schedule_name='customers_every_30m',
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'format': 'systemd',
                'schedule': 'customers_every_30m',
                'service_name': service_name,
                'timer_name': timer_name,
                'service': (
                    '[Unit]\n'
                    'Description=ETLPlus schedule customers_every_30m\n\n'
                    '[Service]\n'
                    'Type=oneshot\n'
                    f'WorkingDirectory={config_path.parent}\n'
                    f'ExecStart=etlplus run --config {config_path} --job job-a\n'
                ),
                'timer': (
                    '[Unit]\n'
                    'Description=Run ETLPlus schedule customers_every_30m\n\n'
                    '[Timer]\n'
                    'OnUnitActiveSec=30m\n'
                    'Persistent=true\n'
                    f'Unit={service_name}\n\n'
                    '[Install]\n'
                    'WantedBy=timers.target\n'
                ),
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

    def test_returns_error_payload_for_unknown_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should fail cleanly when the named schedule is missing."""
        cfg = Config.from_dict(
            {
                'name': 'Schedule Test Pipeline',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [],
            },
        )
        patch_config_from_yaml(monkeypatch, cfg)

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                emit='crontab',
                schedule_name='missing',
            )
            == 1
        )
        assert_emit_json(
            capture_io,
            {
                'message': 'Schedule not found: missing',
                'status': 'error',
            },
            pretty=True,
        )

    def test_run_command_requires_target_and_target_mode(self) -> None:
        """Schedule helper should require a target and one run mode."""
        config_path = Path('cfg.yml').resolve()

        with pytest.raises(ValueError, match='must define a target'):
            schedule_mod._run_command(
                config_path=config_path,
                schedule=SimpleNamespace(name='nightly', target=None),
            )

        with pytest.raises(ValueError, match='must target one job or run_all'):
            schedule_mod._run_command(
                config_path=config_path,
                schedule=SimpleNamespace(
                    name='nightly',
                    target=SimpleNamespace(job=None, run_all=False),
                ),
            )

    def test_run_pending_emits_scheduler_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Schedule handler should delegate due-run execution to the scheduler."""
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
                        'target': {'run_all': True},
                    },
                ],
            },
        )
        captured: dict[str, object] = {}
        patch_config_from_yaml(monkeypatch, cfg)

        monkeypatch.setattr(
            schedule_mod.LocalScheduler,
            'run_pending',
            classmethod(
                lambda _cls, **kwargs: (
                    captured.update(kwargs)
                    or {
                        'checked_at': '2026-05-12T02:00:00+00:00',
                        'dispatched_count': 1,
                        'name': 'Schedule Test Pipeline',
                        'run_count': 1,
                        'runs': [
                            {
                                'job': 'job-a',
                                'run_id': 'run-1',
                                'schedule': 'nightly_all',
                                'status': 'ok',
                                'trigger': 'cron',
                                'triggered_at': '2026-05-12T02:00:00+00:00',
                            },
                        ],
                        'schedule_count': 1,
                        'skipped_count': 0,
                    }
                ),
            ),
        )

        assert (
            handlers.schedule_handler(
                config='cfg.yml',
                event_format='jsonl',
                pretty=False,
                run_pending=True,
                schedule_name='nightly_all',
            )
            == 0
        )
        assert captured['config_path'] == 'cfg.yml'
        assert captured['event_format'] == 'jsonl'
        assert captured['pretty'] is False
        assert captured['schedule_name'] == 'nightly_all'
        assert captured['run_callback'] is schedule_mod._run_handler
        assert_emit_json(
            capture_io,
            {
                'checked_at': '2026-05-12T02:00:00+00:00',
                'dispatched_count': 1,
                'name': 'Schedule Test Pipeline',
                'run_count': 1,
                'runs': [
                    {
                        'job': 'job-a',
                        'run_id': 'run-1',
                        'schedule': 'nightly_all',
                        'status': 'ok',
                        'trigger': 'cron',
                        'triggered_at': '2026-05-12T02:00:00+00:00',
                    },
                ],
                'schedule_count': 1,
                'skipped_count': 0,
            },
            pretty=False,
        )

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

    def test_schedule_emit_helpers_require_compatible_schedule_shapes(self) -> None:
        """Schedule helper emitters should reject missing cron and trigger data."""
        config_path = Path('cfg.yml').resolve()

        with pytest.raises(ValueError, match='must define cron for crontab emission'):
            schedule_mod._crontab_payload(
                config_path=config_path,
                schedule=SimpleNamespace(name='nightly', cron=None),
                working_directory=config_path.parent,
            )

        with pytest.raises(
            ValueError,
            match='must define cron or interval for systemd emission',
        ):
            schedule_mod._systemd_payload(
                config_path=config_path,
                schedule=SimpleNamespace(
                    name='nightly',
                    cron=None,
                    interval=None,
                    target=SimpleNamespace(run_all=True),
                ),
                working_directory=config_path.parent,
            )

    def test_schedule_payload_omits_empty_target_and_backfill_sections(self) -> None:
        """Schedule summaries should skip empty derived target/backfill payloads."""
        cfg = SimpleNamespace(
            name='Schedule Test Pipeline',
            schedules=[
                SimpleNamespace(
                    name='nightly_all',
                    cron='0 2 * * *',
                    target=SimpleNamespace(job=None, run_all=False),
                    interval=None,
                    paused=False,
                    timezone=None,
                    backfill=SimpleNamespace(
                        enabled=False,
                        max_catchup_runs=None,
                        start_at=None,
                    ),
                ),
            ],
        )

        assert schedule_mod._schedule_payload(cast(Config, cfg)) == {
            'name': 'Schedule Test Pipeline',
            'schedule_count': 1,
            'schedules': [
                {
                    'name': 'nightly_all',
                    'cron': '0 2 * * *',
                    'paused': False,
                },
            ],
        }

    def test_schedule_payload_filters_and_includes_backfill_metadata(self) -> None:
        """Schedule summaries should filter by name and include backfill fields."""
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
                        'target': {'run_all': True},
                    },
                    {
                        'name': 'backfill_customers',
                        'interval': {'minutes': 30},
                        'target': {'job': 'job-a'},
                        'backfill': {
                            'enabled': True,
                            'max_catchup_runs': 3,
                            'start_at': '2026-05-01T00:00:00Z',
                        },
                    },
                ],
            },
        )

        assert schedule_mod._schedule_payload(
            cfg,
            schedule_name='backfill_customers',
        ) == {
            'name': 'Schedule Test Pipeline',
            'schedule_count': 1,
            'schedules': [
                {
                    'name': 'backfill_customers',
                    'paused': False,
                    'interval': {'minutes': 30},
                    'target': {'job': 'job-a'},
                    'backfill': {
                        'enabled': True,
                        'max_catchup_runs': 3,
                        'start_at': '2026-05-01T00:00:00Z',
                    },
                },
            ],
        }

    def test_schedule_payload_skips_non_string_schedule_names(self) -> None:
        """Schedule summaries should ignore malformed schedule entries."""
        cfg = SimpleNamespace(
            name='Schedule Test Pipeline',
            schedules=[
                SimpleNamespace(
                    name='nightly_all',
                    cron='0 2 * * *',
                    target=SimpleNamespace(job=None, run_all=True),
                    interval=None,
                    paused=False,
                    timezone=None,
                    backfill=None,
                ),
                SimpleNamespace(
                    name=123,
                    cron='0 3 * * *',
                    target=SimpleNamespace(job=None, run_all=True),
                    interval=None,
                    paused=False,
                    timezone=None,
                    backfill=None,
                ),
            ],
        )

        assert schedule_mod._schedule_payload(cast(Config, cfg)) == {
            'name': 'Schedule Test Pipeline',
            'schedule_count': 1,
            'schedules': [
                {
                    'name': 'nightly_all',
                    'cron': '0 2 * * *',
                    'paused': False,
                    'target': {'run_all': True},
                },
            ],
        }

    def test_schedule_payload_tolerates_missing_target_objects(self) -> None:
        """Schedule summaries should omit target when the schedule has none."""
        cfg = SimpleNamespace(
            name='Schedule Test Pipeline',
            schedules=[
                SimpleNamespace(
                    name='nightly_all',
                    cron='0 2 * * *',
                    target=None,
                    interval=None,
                    paused=False,
                    timezone=None,
                    backfill=None,
                ),
            ],
        )

        assert schedule_mod._schedule_payload(cast(Config, cfg)) == {
            'name': 'Schedule Test Pipeline',
            'schedule_count': 1,
            'schedules': [
                {
                    'name': 'nightly_all',
                    'cron': '0 2 * * *',
                    'paused': False,
                },
            ],
        }

    def test_schedule_slug_falls_back_for_non_alphanumeric_names(self) -> None:
        """Schedule slugs should fall back to a default name when empty."""
        assert schedule_mod._schedule_slug('!!!') == 'schedule'

    def test_systemd_payload_supports_cron_schedules(self) -> None:
        """Systemd helper payloads should emit OnCalendar for cron schedules."""
        config_path = Path('cfg.yml').resolve()

        payload = schedule_mod._systemd_payload(
            config_path=config_path,
            schedule=SimpleNamespace(
                name='weekday_sync',
                cron='5 6 * * 1',
                interval=None,
                target=SimpleNamespace(job='job-a', run_all=False),
            ),
            working_directory=config_path.parent,
        )

        assert payload['format'] == 'systemd'
        assert payload['service_name'] == 'etlplus-weekday-sync.service'
        assert payload['timer_name'] == 'etlplus-weekday-sync.timer'
        assert 'OnCalendar=Mon *-*-* 06:05:00' in cast(str, payload['timer'])
