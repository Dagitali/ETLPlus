"""
:mod:`tests.unit.cli.test_u_cli_commands` module.

Unit tests for :mod:`etlplus.cli._commands`.
"""

from __future__ import annotations

from typing import Any

import pytest
import typer

import etlplus.cli._commands as commands_mod
import etlplus.cli._commands.check as check_mod
import etlplus.cli._commands.helpers as helpers_mod
import etlplus.cli._commands.history as history_mod
import etlplus.cli._commands.log as log_mod
import etlplus.cli._commands.report as report_mod
import etlplus.cli._commands.status as status_mod
import etlplus.cli._commands.transform as transform_mod
import etlplus.cli._state as state_mod
from etlplus.cli._state import CliState
from etlplus.file import FileFormat

from .conftest import AssertCapturedText
from .conftest import InvokeCli
from .conftest import StubHandler
from .conftest import TyperContextFactory

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

        def _parse_json_payload(_value: str) -> Any:
            raise ValueError('bad json')

        monkeypatch.setattr(
            helpers_mod,
            'parse_json_payload',
            _parse_json_payload,
        )
        with pytest.raises(typer.BadParameter, match='Invalid JSON for --ops'):
            commands_mod.parse_json_option('not-json', '--ops')

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


class TestCheckCommand:
    """Unit tests for :func:`etlplus.cli._commands.check_cmd`."""

    def test_delegates_to_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """Test that valid inputs dispatch to ``check_handler``."""
        monkeypatch.setattr(
            check_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured = stub_handler(
            commands_mod.handlers,
            'check_handler',
            result=3,
        )

        result = commands_mod.check_cmd(
            typer_ctx_factory(),
            config='pipeline.yml',
            jobs=True,
            pipelines=False,
            sources=False,
            summary=True,
            targets=False,
            transforms=False,
        )

        assert result == 3
        assert captured['config'] == 'pipeline.yml'
        assert captured['jobs'] is True
        assert captured['pretty'] is False


class TestCliInvokeParsing:
    """Typer runner coverage for history/log/report option parsing."""

    def test_history_until_reaches_handler(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``history --until`` is parsed through the Typer runner."""
        captured: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(history_mod, 'handle_history', _stub)

        result = invoke_cli(
            'history',
            '--until',
            '2026-03-24T00:00:00Z',
        )

        assert result.exit_code == 0
        assert captured['until'] == '2026-03-24T00:00:00Z'

    def test_log_until_reaches_handler(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``log --until`` is parsed through the Typer runner."""
        captured: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(log_mod, 'handle_history', _stub)

        result = invoke_cli(
            'log',
            '--until',
            '2026-03-24T00:00:00Z',
        )

        assert result.exit_code == 0
        assert captured['until'] == '2026-03-24T00:00:00Z'
        assert captured['raw'] is True

    def test_report_group_by_reaches_handler(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``report --group-by`` is parsed through the Typer runner."""
        captured: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(commands_mod.handlers, 'report_handler', _stub)

        result = invoke_cli(
            'report',
            '--group-by',
            'day',
        )

        assert result.exit_code == 0
        assert captured['group_by'] == 'day'

    def test_transform_target_type_reaches_handler(
        self,
        invoke_cli: InvokeCli,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``transform --target-type`` is parsed through Typer."""
        captured: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(commands_mod.handlers, 'transform_handler', _stub)

        result = invoke_cli(
            'transform',
            '--target-type',
            'api',
            'https://example.com/items',
        )

        assert result.exit_code == 0
        assert captured['target_type'] == 'api'


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


class TestHistoryCommand:
    """Unit tests for :func:`etlplus.cli._commands.history_cmd`."""

    def test_delegates_to_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """Test that history inputs dispatch to ``history_handler``."""
        monkeypatch.setattr(
            history_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured = stub_handler(
            commands_mod.handlers,
            'history_handler',
            result=0,
        )
        monkeypatch.setattr(
            history_mod,
            'handle_history',
            commands_mod.handlers.history_handler,
        )

        result = commands_mod.history_cmd(
            typer_ctx_factory(),
            job='job-a',
            limit=5,
            raw=False,
            json_output=True,
            since='2026-03-21T00:00:00Z',
            status='failed',
            table=False,
            until='2026-03-24T00:00:00Z',
        )

        assert result == 0
        assert captured == {
            'job': 'job-a',
            'json_output': True,
            'limit': 5,
            'pretty': False,
            'raw': False,
            'since': '2026-03-21T00:00:00Z',
            'status': 'failed',
            'table': False,
            'until': '2026-03-24T00:00:00Z',
        }


class TestLogCommand:
    """Unit tests for :func:`etlplus.cli._commands.log_cmd`."""

    def test_delegates_to_raw_history_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """Test that log inputs dispatch to the raw history handler."""
        monkeypatch.setattr(
            log_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured = stub_handler(
            commands_mod.handlers,
            'history_handler',
            result=0,
        )
        monkeypatch.setattr(
            log_mod,
            'handle_history',
            commands_mod.handlers.history_handler,
        )

        result = commands_mod.log_cmd(
            typer_ctx_factory(),
            follow=True,
            limit=7,
            run_id='run-7',
            since='2026-03-23T00:00:00Z',
            until='2026-03-24T00:00:00Z',
        )

        assert result == 0
        assert captured == {
            'follow': True,
            'limit': 7,
            'pretty': False,
            'raw': True,
            'run_id': 'run-7',
            'since': '2026-03-23T00:00:00Z',
            'until': '2026-03-24T00:00:00Z',
        }


class TestReportCommand:
    """Unit tests for :func:`etlplus.cli._commands.report_cmd`."""

    def test_delegates_to_report_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """Test that report inputs dispatch to ``report_handler``."""
        monkeypatch.setattr(
            report_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured = stub_handler(
            commands_mod.handlers,
            'report_handler',
            result=0,
        )

        result = commands_mod.report_cmd(
            typer_ctx_factory(),
            group_by='status',
            job='job-a',
            json_output=True,
            since='2026-03-01T00:00:00Z',
            table=False,
            until='2026-03-31T23:59:59Z',
        )

        assert result == 0
        assert captured == {
            'group_by': 'status',
            'job': 'job-a',
            'json_output': True,
            'pretty': False,
            'since': '2026-03-01T00:00:00Z',
            'table': False,
            'until': '2026-03-31T23:59:59Z',
        }


class TestStatusCommand:
    """Unit tests for :func:`etlplus.cli._commands.status_cmd`."""

    def test_delegates_to_status_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
        typer_ctx_factory: TyperContextFactory,
        stub_handler: StubHandler,
    ) -> None:
        """Test that status inputs dispatch to ``status_handler``."""
        monkeypatch.setattr(
            status_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured = stub_handler(
            commands_mod.handlers,
            'status_handler',
            result=0,
        )

        result = commands_mod.status_cmd(
            typer_ctx_factory(),
            job='job-a',
            run_id='run-9',
        )

        assert result == 0
        assert captured == {
            'job': 'job-a',
            'pretty': False,
            'run_id': 'run-9',
        }


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
            commands_mod.handlers,
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
