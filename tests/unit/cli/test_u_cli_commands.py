"""
:mod:`tests.unit.cli.test_u_cli_commands` module.

Unit tests for :mod:`etlplus.cli._commands`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

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
import etlplus.cli._commands.status as status_mod
import etlplus.cli._commands.transform as transform_mod
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
            'call_handler',
            'fail_usage',
            'normalize_file_format',
            'parse_json_option',
            'require_any',
            'require_value',
            'resolve_resource',
        ]

    def test_call_handler_injects_requested_state_fields(self) -> None:
        """Shared handler dispatch should merge selected CLI state fields."""
        captured: dict[str, object] = {}

        def _handler(**kwargs: object) -> int:
            captured.update(kwargs)
            return 7

        result = helpers_mod.call_handler(
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

    def test_normalize_file_format_returns_enum_member(self) -> None:
        """Shared format normalization should preserve ``FileFormat`` typing."""
        assert helpers_mod.normalize_file_format('json', label='source') is (
            FileFormat.JSON
        )

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
            helpers_mod,
            'parse_json',
            _parse_json,
        )
        with pytest.raises(typer.BadParameter, match='Invalid JSON for --ops'):
            helpers_mod.parse_json_option('not-json', '--ops')

    def test_resolve_logged_resource_type_reuses_state_implementation(
        self,
    ) -> None:
        """Command helpers should reuse the shared state implementation."""
        assert (
            helpers_mod._resolve_logged_resource_type
            is state_mod.resolve_logged_resource_type
        )

    def test_resolve_resource_normalizes_type_and_format(self) -> None:
        """Shared resource resolution should normalize type and format hints."""
        resolved = helpers_mod.resolve_resource(
            CliState(),
            role='source',
            value='payload.json',
            connector_type='API',
            format_value='json',
        )

        assert resolved.value == 'payload.json'
        assert resolved.resource_type == 'api'
        assert resolved.format_hint is FileFormat.JSON


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
                    'event_format': 'jsonl',
                },
                {
                    'config': 'pipeline.yml',
                    'job': 'job-a',
                    'pipeline': None,
                    'run_all': False,
                    'continue_on_fail': True,
                    'event_format': 'jsonl',
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
                    'follow': True,
                    'limit': 7,
                    'run_id': 'run-7',
                    'since': '2026-03-23T00:00:00Z',
                    'until': '2026-03-24T00:00:00Z',
                },
                {
                    'follow': True,
                    'limit': 7,
                    'pretty': False,
                    'raw': True,
                    'run_id': 'run-7',
                    'since': '2026-03-23T00:00:00Z',
                    'until': '2026-03-24T00:00:00Z',
                },
                0,
                id='log',
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
            module,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
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
            check_mod,
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
            check_mod,
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
            run_mod,
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
                    'run',
                    '--config',
                    'pipeline.yml',
                    '--all',
                    '--continue-on-fail',
                ),
                run_mod,
                'run_handler',
                {
                    'config': 'pipeline.yml',
                    'continue_on_fail': True,
                    'run_all': True,
                },
                id='run-all',
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
            transform_mod,
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
