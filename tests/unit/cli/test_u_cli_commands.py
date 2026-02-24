"""
:mod:`tests.unit.cli.test_u_cli_commands` module.

Unit tests for :mod:`etlplus.cli.commands`.
"""

from __future__ import annotations

from typing import Any

import pytest
import typer

import etlplus.cli.commands as commands_mod
from etlplus.cli.state import CliState

# SECTION: HELPERS ========================================================== #


def _ctx() -> typer.Context:
    command = typer.main.get_command(commands_mod.app)
    return typer.Context(command)


# SECTION: TESTS ============================================================ #


class TestCommandsInternalHelpers:
    """Unit tests for command-level internal helper functions."""

    # pylint: disable=protected-access

    def test_parse_json_option_wraps_value_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Invalid JSON payloads should raise :class:`typer.BadParameter`."""

        def _parse_json_payload(_value: str) -> Any:
            raise ValueError('bad json')

        monkeypatch.setattr(
            commands_mod,
            'parse_json_payload',
            _parse_json_payload,
        )
        with pytest.raises(typer.BadParameter, match='Invalid JSON for --ops'):
            commands_mod._parse_json_option('not-json', '--ops')


class TestCheckCommand:
    """Unit tests for :func:`etlplus.cli.commands.check_cmd`."""

    def test_delegates_to_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Valid inputs should dispatch to ``check_handler``."""
        monkeypatch.setattr(
            commands_mod,
            'ensure_state',
            lambda _ctx: CliState(pretty=False),
        )
        captured: dict[str, Any] = {}

        def _check_handler(**kwargs: Any) -> int:
            captured.update(kwargs)
            return 3

        monkeypatch.setattr(
            commands_mod.handlers,
            'check_handler',
            _check_handler,
        )

        result = commands_mod.check_cmd(
            _ctx(),
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

    def test_requires_config_option(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Empty config should raise :class:`typer.Exit` with code 2."""
        with pytest.raises(typer.Exit) as exc:
            commands_mod.check_cmd(_ctx(), config='')
        assert exc.value.exit_code == 2
        assert "Missing required option '--config'" in capsys.readouterr().err


class TestExtractCommand:
    """Unit tests for :func:`etlplus.cli.commands.extract_cmd`."""

    @pytest.mark.parametrize(
        ('source', 'expected_message'),
        [
            (
                '--oops',
                "must follow the 'SOURCE' argument",
            ),
            (
                '',
                "Missing required argument 'SOURCE'",
            ),
        ],
    )
    def test_rejects_invalid_source_argument_ordering(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        source: str,
        expected_message: str,
    ) -> None:
        """Invalid SOURCE values should produce exit code 2."""
        monkeypatch.setattr(
            commands_mod,
            'ensure_state',
            lambda _ctx: CliState(),
        )
        with pytest.raises(typer.Exit) as exc:
            commands_mod.extract_cmd(_ctx(), source=source)
        assert exc.value.exit_code == 2
        assert expected_message in capsys.readouterr().err


class TestLoadCommand:
    """Unit tests for :func:`etlplus.cli.commands.load_cmd`."""

    @pytest.mark.parametrize(
        ('target', 'expected_message'),
        [
            (
                '--oops',
                "must follow the 'TARGET' argument",
            ),
            (
                '',
                "Missing required argument 'TARGET'",
            ),
        ],
    )
    def test_rejects_invalid_target_argument_ordering(
        self,
        capsys: pytest.CaptureFixture[str],
        target: str,
        expected_message: str,
    ) -> None:
        """Invalid TARGET values should produce exit code 2."""
        with pytest.raises(typer.Exit) as exc:
            commands_mod.load_cmd(_ctx(), target=target)
        assert exc.value.exit_code == 2
        assert expected_message in capsys.readouterr().err


class TestRenderCommand:
    """Unit tests for :func:`etlplus.cli.commands.render_cmd`."""

    def test_requires_config_or_spec(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Missing both ``--config`` and ``--spec`` should exit with code 2."""
        with pytest.raises(typer.Exit) as exc:
            commands_mod.render_cmd(_ctx(), config=None, spec=None)
        assert exc.value.exit_code == 2
        assert (
            "Missing required option '--config' or '--spec'"
            in capsys.readouterr().err
        )


class TestRunCommand:
    """Unit tests for :func:`etlplus.cli.commands.run_cmd`."""

    def test_requires_config_option(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Empty config should raise :class:`typer.Exit` with code 2."""
        with pytest.raises(typer.Exit) as exc:
            commands_mod.run_cmd(_ctx(), config='')
        assert exc.value.exit_code == 2
        assert "Missing required option '--config'" in capsys.readouterr().err


class TestTransformCommand:
    """Unit tests for :func:`etlplus.cli.commands.transform_cmd`."""

    # pylint: disable=unused-argument

    def test_skips_source_validation_when_source_type_cannot_be_inferred(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        When source type is ``None``, source validation should be skipped.
        """
        monkeypatch.setattr(
            commands_mod,
            'ensure_state',
            lambda _ctx: CliState(),
        )
        monkeypatch.setattr(
            commands_mod,
            'optional_choice',
            lambda value, choices, label: value,
        )
        monkeypatch.setattr(
            commands_mod,
            'infer_resource_type_soft',
            lambda _source: None,
        )
        monkeypatch.setattr(
            commands_mod,
            'resolve_resource_type',
            lambda **kwargs: 'file',
        )
        monkeypatch.setattr(
            commands_mod,
            '_parse_json_option',
            lambda value, flag: {},
        )
        validate_called = {'count': 0}

        def _validate_choice(
            value: str | object,
            choices: set[str] | frozenset[str],
            *,
            label: str,
        ) -> str:
            validate_called['count'] += 1
            return str(value)

        monkeypatch.setattr(commands_mod, 'validate_choice', _validate_choice)
        captured: dict[str, Any] = {}

        def _transform_handler(**kwargs: Any) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr(
            commands_mod.handlers,
            'transform_handler',
            _transform_handler,
        )

        result = commands_mod.transform_cmd(
            _ctx(),
            operations='{}',
            source='payload',
            source_format=None,
            source_type=None,
            target='-',
            target_format=None,
            target_type=None,
        )

        assert result == 0
        assert validate_called['count'] == 0
        assert captured['source'] == 'payload'
