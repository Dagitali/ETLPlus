"""
:mod:`tests.unit.cli.test_u_cli_commands` module.

Unit tests for :mod:`etlplus.cli._commands`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast

import pytest
import typer

import etlplus.cli._commands as commands_mod
import etlplus.cli._commands._helpers as helpers_mod
import etlplus.cli._commands._state as state_mod
import etlplus.cli._commands.check as check_mod
import etlplus.cli._commands.history as history_mod
import etlplus.cli._commands.init as init_mod
import etlplus.cli._commands.log as log_mod
import etlplus.cli._commands.report as report_mod
import etlplus.cli._commands.run as run_mod
import etlplus.cli._commands.schedule as schedule_mod
import etlplus.cli._commands.status as status_mod
import etlplus.cli._commands.transform as transform_mod
import etlplus.cli._handlers.schedule as schedule_handler_mod
from etlplus.cli._commands._state import CliState
from etlplus.file import FileFormat

from .conftest import AssertCapturedText
from .conftest import InvokeCli
from .conftest import StubHandler
from .conftest import TyperContextFactory
from .conftest import assert_mapping_contains

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCommandsInternalHelpers:
    """Unit tests for command-level internal helper functions."""

    def test_helpers_export_intentional_public_api(self) -> None:
        """Command helpers should expose only the intended public surface."""
        assert helpers_mod.__all__ == [
            'CommandHelperPolicy',
        ]

    def test_call_handler_injects_requested_state_fields(self) -> None:
        """Shared handler dispatch should merge selected CLI state fields."""
        captured: dict[str, object] = {}

        def _handler(**kwargs: object) -> int:
            captured.update(kwargs)
            return 7

        result = helpers_mod.CommandHelperPolicy.call_handler(
            _handler,
            state=CliState(pretty=False, quiet=True, verbose=True),
            state_fields=('pretty', 'quiet'),
            value='payload',
        )

        assert result == 7
        assert captured == {
            'pretty': False,
            'quiet': True,
            'value': 'payload',
        }

    def test_call_history_command_omits_unset_filters_and_preserves_explicit_none(
        self,
    ) -> None:
        """History dispatch should forward only explicit filters plus ``pretty``."""
        captured: dict[str, object] = {}

        def _handler(**kwargs: object) -> int:
            captured.update(kwargs)
            return 11

        result = helpers_mod.CommandHelperPolicy.call_history_command(
            _handler,
            ctx=cast(typer.Context, object()),
            state=CliState(pretty=False),
            level='job',
            job='seed',
            pipeline=None,
            follow=True,
        )

        assert result == 11
        assert captured == {
            'follow': True,
            'job': 'seed',
            'level': 'job',
            'pipeline': None,
            'pretty': False,
        }

    def test_call_history_command_reuses_supplied_state(self) -> None:
        """History command dispatch should reuse injected CLI state."""
        captured: dict[str, object] = {}

        def _handler(**kwargs: object) -> int:
            captured.update(kwargs)
            return 13

        result = helpers_mod.CommandHelperPolicy.call_history_command(
            _handler,
            ctx=cast(typer.Context, object()),
            state=CliState(pretty=False),
            level='run',
            status='failed',
        )

        assert result == 13
        assert captured == {
            'level': 'run',
            'pretty': False,
            'status': 'failed',
        }

    @pytest.mark.parametrize(
        'format_value',
        [
            pytest.param('json', id='str'),
            pytest.param(FileFormat.JSON, id='enum'),
        ],
    )
    def test_resolve_resource_preserves_file_format_typing(
        self,
        format_value: FileFormat | str,
    ) -> None:
        """Shared resource resolution should preserve ``FileFormat`` typing."""
        resolved = helpers_mod.CommandHelperPolicy.resolve_resource(
            CliState(),
            role='source',
            value='payload.json',
            connector_type='file',
            format_value=format_value,
        )

        assert resolved.format_hint is FileFormat.JSON

    def test_parse_json_option_wraps_value_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that invalid JSON payloads raise :class:`typer.BadParameter`.
        """

        def _parse_json(_value: str) -> Any:
            raise ValueError('bad json')

        monkeypatch.setattr(
            helpers_mod.JsonCodec,
            'parse',
            _parse_json,
        )
        with pytest.raises(typer.BadParameter, match='Invalid JSON for --ops'):
            helpers_mod.CommandHelperPolicy.parse_json_option(
                'not-json',
                '--ops',
            )

    def test_resolve_resource_reuses_supplied_state(self) -> None:
        """Command resource resolution should reuse the injected CLI state."""
        state = CliState(pretty=False)

        resolved = helpers_mod.CommandHelperPolicy.resolve_resource(
            state,
            role='source',
            value='payload.json',
            connector_type='file',
            format_value='json',
        )
        assert resolved.require_resource_type() == 'file'
        assert resolved.format_explicit is True

    def test_resolve_logged_resource_type_reuses_state_implementation(
        self,
    ) -> None:
        """Command helpers should reuse the shared state implementation."""
        assert (
            helpers_mod._resolve_logged_resource_type
            is state_mod.resolve_logged_resource_type
        )

    @pytest.mark.parametrize(
        'value',
        [
            pytest.param(None, id='missing'),
            pytest.param('', id='empty'),
        ],
    )
    def test_require_value_rejects_missing_values(
        self,
        value: str | None,
    ) -> None:
        """Required CLI values should fail through Typer usage errors."""
        with pytest.raises(typer.Exit) as exc_info:
            helpers_mod.CommandHelperPolicy.require_value(
                value,
                message="Missing required argument 'SOURCE'.",
            )

        assert exc_info.value.exit_code == 2

    def test_require_value_rejects_option_like_positionals(self) -> None:
        """Positionals should not silently consume option-looking values."""
        with pytest.raises(typer.Exit) as exc_info:
            helpers_mod.CommandHelperPolicy.require_value(
                '--target',
                message="Missing required argument 'SOURCE'.",
                positional_name='SOURCE',
            )

        assert exc_info.value.exit_code == 2

    def test_resolve_resource_normalizes_type_and_format(self) -> None:
        """Shared resource resolution should normalize type and format hints."""
        resolved = helpers_mod.CommandHelperPolicy.resolve_resource(
            CliState(),
            role='source',
            value='payload.json',
            connector_type='API',
            format_value='json',
        )

        assert resolved.value == 'payload.json'
        assert resolved.resource_type == 'api'
        assert resolved.format_hint is FileFormat.JSON

    @pytest.mark.parametrize(
        ('value', 'expected_message'),
        [
            pytest.param(
                'payload.json',
                "No connector type resolved for 'payload.json'",
                id='file-path',
            ),
            pytest.param(
                '-',
                "No connector type resolved for '-'",
                id='stdio',
            ),
        ],
    )
    def test_resolved_resource_requires_resource_type(
        self,
        value: str,
        expected_message: str,
    ) -> None:
        """Missing connector types should fail explicitly instead of asserting."""
        resolved = helpers_mod._ResolvedResource(value=value)

        with pytest.raises(ValueError, match=expected_message):
            resolved.require_resource_type()


class TestDelegatingCommands:
    """Unit tests for thin command wrappers that delegate to handlers."""

    @pytest.mark.parametrize(
        (
            'module',
            'command',
            'handler_name',
            'kwargs',
            'expected',
            'result',
        ),
        [
            pytest.param(
                check_mod,
                commands_mod.check_cmd,
                'check_handler',
                {
                    'config': 'pipeline.yml',
                    'graph': False,
                    'jobs': True,
                    'pipelines': False,
                    'sources': False,
                    'summary': True,
                    'targets': False,
                    'transforms': False,
                },
                {
                    'config': 'pipeline.yml',
                    'graph': False,
                    'jobs': True,
                    'pipelines': False,
                    'readiness': False,
                    'sources': False,
                    'strict': False,
                    'summary': True,
                    'targets': False,
                    'transforms': False,
                    'pretty': False,
                },
                3,
                id='check',
            ),
            pytest.param(
                run_mod,
                commands_mod.run_cmd,
                'run_handler',
                {
                    'config': 'pipeline.yml',
                    'job': 'job-a',
                    'pipeline': None,
                    'run_all': False,
                    'continue_on_fail': True,
                    'max_concurrency': 3,
                    'event_format': 'jsonl',
                },
                {
                    'capture_tracebacks': None,
                    'config': 'pipeline.yml',
                    'job': 'job-a',
                    'pipeline': None,
                    'run_all': False,
                    'continue_on_fail': True,
                    'max_concurrency': 3,
                    'event_format': 'jsonl',
                    'history_backend': None,
                    'history_enabled': None,
                    'history_state_dir': None,
                    'pretty': False,
                },
                0,
                id='run',
            ),
            pytest.param(
                history_mod,
                commands_mod.history_cmd,
                'history_handler',
                {
                    'job': 'job-a',
                    'limit': 5,
                    'raw': False,
                    'json_output': True,
                    'since': '2026-03-21T00:00:00Z',
                    'status': 'failed',
                    'table': False,
                    'until': '2026-03-24T00:00:00Z',
                },
                {
                    'job': 'job-a',
                    'json_output': True,
                    'level': 'run',
                    'limit': 5,
                    'pipeline': None,
                    'pretty': False,
                    'raw': False,
                    'run_id': None,
                    'since': '2026-03-21T00:00:00Z',
                    'status': 'failed',
                    'table': False,
                    'until': '2026-03-24T00:00:00Z',
                },
                0,
                id='history',
            ),
            pytest.param(
                init_mod,
                commands_mod.init_cmd,
                'init_handler',
                {
                    'directory': 'demo',
                    'force': True,
                },
                {
                    'directory': 'demo',
                    'force': True,
                    'pretty': False,
                },
                5,
                id='init',
            ),
            pytest.param(
                log_mod,
                commands_mod.log_cmd,
                'history_handler',
                {
                    'job': 'job-a',
                    'level': 'job',
                    'follow': True,
                    'limit': 7,
                    'pipeline': 'pipeline-a',
                    'run_id': 'run-7',
                    'since': '2026-03-23T00:00:00Z',
                    'status': 'skipped',
                    'until': '2026-03-24T00:00:00Z',
                },
                {
                    'follow': True,
                    'job': 'job-a',
                    'level': 'job',
                    'limit': 7,
                    'pipeline': 'pipeline-a',
                    'pretty': False,
                    'raw': True,
                    'run_id': 'run-7',
                    'since': '2026-03-23T00:00:00Z',
                    'status': 'skipped',
                    'until': '2026-03-24T00:00:00Z',
                },
                0,
                id='log',
            ),
            pytest.param(
                schedule_mod,
                commands_mod.schedule_cmd,
                'schedule_handler',
                {
                    'config': 'pipeline.yml',
                    'emit': 'crontab',
                    'schedule': 'nightly_all',
                },
                {
                    'config': 'pipeline.yml',
                    'emit': 'crontab',
                    'pretty': False,
                    'schedule_name': 'nightly_all',
                },
                0,
                id='schedule',
            ),
            pytest.param(
                report_mod,
                commands_mod.report_cmd,
                'report_handler',
                {
                    'group_by': 'status',
                    'job': 'job-a',
                    'json_output': True,
                    'since': '2026-03-01T00:00:00Z',
                    'table': False,
                    'until': '2026-03-31T23:59:59Z',
                },
                {
                    'group_by': 'status',
                    'job': 'job-a',
                    'json_output': True,
                    'level': 'run',
                    'pipeline': None,
                    'pretty': False,
                    'run_id': None,
                    'since': '2026-03-01T00:00:00Z',
                    'status': None,
                    'table': False,
                    'until': '2026-03-31T23:59:59Z',
                },
                0,
                id='report',
            ),
            pytest.param(
                status_mod,
                commands_mod.status_cmd,
                'status_handler',
                {
                    'job': 'job-a',
                    'run_id': 'run-9',
                },
                {
                    'job': 'job-a',
                    'level': 'run',
                    'pipeline': None,
                    'pretty': False,
                    'run_id': 'run-9',
                },
                0,
                id='status',
            ),
        ],
    )
    def test_delegates_to_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
        module: object,
        command: Callable[..., int],
        handler_name: str,
        kwargs: dict[str, object],
        expected: dict[str, object],
        result: int,
    ) -> None:
        """
        Test that thin command wrappers forward normalized kwargs verbatim.
        """
        monkeypatch.setattr(
            helpers_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        monkeypatch.setattr(
            module,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
            raising=False,
        )
        captured = stub_handler(
            module,
            handler_name,
            result=result,
        )

        assert command(typer_ctx_factory(), **kwargs) == result
        assert captured == expected

    def test_rejects_graph_with_inspection_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """Graph mode should reject section-inspection flag combinations."""
        monkeypatch.setattr(
            check_mod.CommandHelperPolicy,
            'fail_usage',
            lambda message: (_ for _ in ()).throw(typer.BadParameter(message)),
        )

        with pytest.raises(
            typer.BadParameter,
            match='--graph cannot be combined with inspection flags',
        ):
            commands_mod.check_cmd(
                typer_ctx_factory(),
                config='pipeline.yml',
                graph=True,
                jobs=True,
            )

    def test_rejects_readiness_with_inspection_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """Readiness mode should reject inspection-flag combinations."""
        monkeypatch.setattr(
            check_mod.CommandHelperPolicy,
            'fail_usage',
            lambda message: (_ for _ in ()).throw(typer.BadParameter(message)),
        )

        with pytest.raises(
            typer.BadParameter,
            match='--readiness cannot be combined with inspection flags',
        ):
            commands_mod.check_cmd(
                typer_ctx_factory(),
                config='pipeline.yml',
                jobs=True,
                readiness=True,
            )

    def test_rejects_run_all_with_job_selection(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """Run mode should reject ``--all`` combined with job selection."""
        monkeypatch.setattr(
            run_mod.CommandHelperPolicy,
            'fail_usage',
            lambda message: (_ for _ in ()).throw(typer.BadParameter(message)),
        )

        with pytest.raises(
            typer.BadParameter,
            match='--all cannot be combined with --job or --pipeline',
        ):
            commands_mod.run_cmd(
                typer_ctx_factory(),
                config='pipeline.yml',
                job='job-a',
                run_all=True,
            )

    def test_schedule_emit_requires_named_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
    ) -> None:
        """Schedule helper emission should require one named schedule."""
        monkeypatch.setattr(
            schedule_mod.CommandHelperPolicy,
            'fail_usage',
            lambda message: (_ for _ in ()).throw(typer.BadParameter(message)),
        )

        with pytest.raises(
            typer.BadParameter,
            match="'--emit' requires '--schedule'",
        ):
            commands_mod.schedule_cmd(
                typer_ctx_factory(),
                config='pipeline.yml',
                emit='crontab',
            )


class TestCliInvokeParsing:
    """Typer runner coverage for history/log/report option parsing."""

    @pytest.mark.parametrize(
        ('argv', 'module', 'handler_name', 'expected'),
        [
            pytest.param(
                ('check', '--config', 'pipeline.yml', '--graph'),
                check_mod,
                'check_handler',
                {'graph': True},
                id='check-graph',
            ),
            pytest.param(
                ('history', '--until', '2026-03-24T00:00:00Z'),
                history_mod,
                'history_handler',
                {'until': '2026-03-24T00:00:00Z'},
                id='history-until',
            ),
            pytest.param(
                (
                    'history',
                    '--level',
                    'job',
                    '--pipeline',
                    'pipeline-a',
                ),
                history_mod,
                'history_handler',
                {
                    'level': 'job',
                    'pipeline': 'pipeline-a',
                },
                id='history-level-job-pipeline',
            ),
            pytest.param(
                ('log', '--until', '2026-03-24T00:00:00Z'),
                log_mod,
                'history_handler',
                {
                    'until': '2026-03-24T00:00:00Z',
                    'raw': True,
                },
                id='log-until',
            ),
            pytest.param(
                (
                    'log',
                    '--level',
                    'job',
                    '--pipeline',
                    'pipeline-a',
                    '--status',
                    'skipped',
                ),
                log_mod,
                'history_handler',
                {
                    'level': 'job',
                    'pipeline': 'pipeline-a',
                    'raw': True,
                    'status': 'skipped',
                },
                id='log-level-job-pipeline-status',
            ),
            pytest.param(
                (
                    'schedule',
                    '--config',
                    'pipeline.yml',
                    '--schedule',
                    'nightly_all',
                    '--emit',
                    'systemd',
                ),
                schedule_mod,
                'schedule_handler',
                {
                    'config': 'pipeline.yml',
                    'emit': 'systemd',
                    'schedule_name': 'nightly_all',
                },
                id='schedule-emit-systemd',
            ),
            pytest.param(
                (
                    'run',
                    '--config',
                    'pipeline.yml',
                    '--all',
                    '--max-concurrency',
                    '2',
                    '--continue-on-fail',
                ),
                run_mod,
                'run_handler',
                {
                    'config': 'pipeline.yml',
                    'continue_on_fail': True,
                    'max_concurrency': 2,
                    'run_all': True,
                },
                id='run-all',
            ),
            pytest.param(
                (
                    'run',
                    '--config',
                    'pipeline.yml',
                    '--job',
                    'job-a',
                    '--no-history',
                    '--history-backend',
                    'jsonl',
                    '--history-state-dir',
                    '.etlplus-state',
                    '--capture-tracebacks',
                ),
                run_mod,
                'run_handler',
                {
                    'capture_tracebacks': True,
                    'config': 'pipeline.yml',
                    'history_backend': 'jsonl',
                    'history_enabled': False,
                    'history_state_dir': '.etlplus-state',
                    'job': 'job-a',
                },
                id='run-history-overrides',
            ),
            pytest.param(
                ('report', '--group-by', 'day'),
                report_mod,
                'report_handler',
                {'group_by': 'day'},
                id='report-group-by',
            ),
            pytest.param(
                (
                    'transform',
                    '--target-type',
                    'api',
                    'https://example.com/items',
                ),
                transform_mod,
                'transform_handler',
                {'target_type': 'api'},
                id='transform-target-type',
            ),
        ],
    )
    def test_parsed_options_reach_handler(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
        argv: tuple[str, ...],
        module: object,
        handler_name: str,
        expected: dict[str, object],
    ) -> None:
        """Typer parsing should pass normalized option values to handlers."""
        captured: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(module, handler_name, _stub)

        result = invoke_cli(*argv)
        assert result.exit_code == 0
        assert_mapping_contains(captured, expected)

    def test_schedule_run_pending_persists_state_across_cli_invocations(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Repeated CLI schedule dispatch should not re-run the same due trigger."""
        config_path = tmp_path / 'pipeline.yml'
        config_path.write_text(
            '\n'.join(
                (
                    'name: CLI Scheduler Test',
                    'sources: []',
                    'targets: []',
                    'jobs: []',
                    'schedules:',
                    '  - name: nightly_all',
                    '    cron: "0 2 11 5 1"',
                    '    timezone: UTC',
                    '    target:',
                    '      run_all: true',
                    '',
                ),
            ),
            encoding='utf-8',
        )
        state_dir = tmp_path / 'state'
        dispatch_calls: list[dict[str, object]] = []

        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))
        monkeypatch.setattr(
            schedule_handler_mod.LocalScheduler,
            'utc_now',
            staticmethod(lambda: datetime(2026, 5, 11, 2, 0, tzinfo=UTC)),
        )

        def _stub_run_handler(**kwargs: object) -> int:
            dispatch_calls.append(dict(kwargs))
            recorder = kwargs.get('result_recorder')
            assert callable(recorder)
            recorder({'run_id': f'run-{len(dispatch_calls)}', 'status': 'ok'})
            return 0

        monkeypatch.setattr(schedule_handler_mod, '_run_handler', _stub_run_handler)

        first_result = invoke_cli(
            'schedule',
            '--config',
            str(config_path),
            '--run-pending',
        )
        second_result = invoke_cli(
            'schedule',
            '--config',
            str(config_path),
            '--run-pending',
        )

        assert first_result.exit_code == 0
        assert second_result.exit_code == 0
        assert len(dispatch_calls) == 1

        first_payload = json.loads(first_result.stdout)
        second_payload = json.loads(second_result.stdout)

        assert first_payload['dispatched_count'] == 1
        assert first_payload['run_count'] == 1
        assert first_payload['runs'][0]['run_id'] == 'run-1'
        assert second_payload['dispatched_count'] == 0
        assert second_payload['run_count'] == 0
        assert second_payload['schedule_count'] == 1
        assert json.loads((state_dir / 'scheduler-state.json').read_text()) == {
            'schedules': {
                'nightly_all': {
                    'last_triggered_at': '2026-05-11T02:00:00+00:00',
                },
            },
        }


class TestCommandsMissingInputs:
    """Unit tests for missing required args/options."""

    @pytest.mark.parametrize(
        ('command_name', 'kwargs', 'expected_message'),
        [
            pytest.param(
                'check_cmd',
                {'config': ''},
                "Missing required option '--config'",
                id='check-missing-config',
            ),
            pytest.param(
                'run_cmd',
                {'config': ''},
                "Missing required option '--config'",
                id='run-missing-config',
            ),
            pytest.param(
                'render_cmd',
                {'config': None, 'spec': None},
                "Missing required option '--config' or '--spec'",
                id='render-missing-input',
            ),
            pytest.param(
                'extract_cmd',
                {'source': ''},
                "Missing required argument 'SOURCE'",
                id='extract-missing-source',
            ),
            pytest.param(
                'load_cmd',
                {'target': ''},
                "Missing required argument 'TARGET'",
                id='load-missing-target',
            ),
        ],
    )
    def test_missing_required_inputs_exit_with_usage_error(
        self,
        typer_ctx_factory: TyperContextFactory,
        assert_stderr_contains: AssertCapturedText,
        command_name: str,
        kwargs: dict[str, object],
        expected_message: str,
    ) -> None:
        """
        Test that commands emit friendly usage errors when required inputs are
        missing.
        """
        command = getattr(commands_mod, command_name)
        with pytest.raises(typer.Exit) as exc:
            command(typer_ctx_factory(), **kwargs)
        assert exc.value.exit_code == 2
        assert_stderr_contains(expected_message)

    @pytest.mark.parametrize(
        (
            'command_name',
            'argument_name',
            'argument_value',
            'expected_message',
        ),
        [
            pytest.param(
                'extract_cmd',
                'source',
                '--oops',
                "must follow the 'SOURCE' argument",
                id='extract-option-before-source',
            ),
            pytest.param(
                'load_cmd',
                'target',
                '--oops',
                "must follow the 'TARGET' argument",
                id='load-option-before-target',
            ),
        ],
    )
    def test_rejects_option_values_for_positional_arguments(
        self,
        typer_ctx_factory: TyperContextFactory,
        assert_stderr_contains: AssertCapturedText,
        command_name: str,
        argument_name: str,
        argument_value: str,
        expected_message: str,
    ) -> None:
        """Test that positional arguments reject option-like values."""
        kwargs = {argument_name: argument_value}
        command = getattr(commands_mod, command_name)
        with pytest.raises(typer.Exit) as exc:
            command(typer_ctx_factory(), **kwargs)
        assert exc.value.exit_code == 2
        assert_stderr_contains(expected_message)


class TestTransformCommand:
    """Unit tests for :func:`etlplus.cli._commands.transform_cmd`."""

    def test_allows_unknown_source_type_when_soft_inference_returns_none(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """
        Test that, when source type is ``None``, source validation is skipped.
        """
        monkeypatch.setattr(
            transform_mod,
            'ensure_state',
            lambda _ctx: CliState(),
        )
        monkeypatch.setattr(
            transform_mod.CommandHelperPolicy,
            'parse_json_option',
            lambda value, flag: {},
        )
        resolver_calls: list[dict[str, object]] = []

        def resolve_logged_resource_type(
            state: CliState,
            *,
            role: str,
            value: str,
            explicit_type: str | None,
            soft_inference: bool = False,
        ) -> str | None:
            resolver_calls.append(
                {
                    'state': state,
                    'role': role,
                    'value': value,
                    'explicit_type': explicit_type,
                    'soft_inference': soft_inference,
                },
            )
            return None if role == 'source' else 'file'

        monkeypatch.setattr(
            helpers_mod,
            '_resolve_logged_resource_type',
            resolve_logged_resource_type,
        )
        captured = stub_handler(
            transform_mod,
            'transform_handler',
            result=0,
        )

        result = commands_mod.transform_cmd(
            typer_ctx_factory(),
            operations='{}',
            source='payload',
            source_format=None,
            source_type=None,
            target='-',
            target_format=None,
            target_type=None,
        )

        assert result == 0
        assert resolver_calls == [
            {
                'state': CliState(),
                'role': 'source',
                'value': 'payload',
                'explicit_type': None,
                'soft_inference': True,
            },
            {
                'state': CliState(),
                'role': 'target',
                'value': '-',
                'explicit_type': None,
                'soft_inference': False,
            },
        ]
        assert captured['source'] == 'payload'
        assert captured['target_type'] == 'file'
