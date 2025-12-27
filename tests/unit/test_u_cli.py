"""
:mod:`tests.unit.test_u_cli` module.

Unit tests for :mod:`etlplus.cli`.

Notes
-----
- These tests are hermetic; they perform no real file or network I/O.
"""

from __future__ import annotations

import argparse
import types
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

import etlplus.cli as cli

# SECTION: HELPERS ========================================================== #

pytestmark = pytest.mark.unit

type ParseCli = Callable[[Sequence[str]], argparse.Namespace]


@dataclass(frozen=True, slots=True)
class ParserCase:
    """
    Declarative CLI parser test case.

    Attributes
    ----------
    identifier : str
        Stable ID for pytest parametrization.
    args : tuple[str, ...]
        Argument vector passed to :meth:`argparse.ArgumentParser.parse_args`.
    expected : Mapping[str, object]
        Mapping of expected attribute values on the returned namespace.
    """

    identifier: str
    args: tuple[str, ...]
    expected: Mapping[str, object]


# Shared parser cases to keep parametrization DRY and self-documenting.
CLI_CASES: Final[tuple[ParserCase, ...]] = (
    ParserCase(
        identifier='extract-default-format',
        args=('extract', 'file', '/path/to/file.json'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.json',
            'format': 'json',
        },
    ),
    ParserCase(
        identifier='extract-explicit-format',
        args=('extract', 'file', '/path/to/file.csv', '--format', 'csv'),
        expected={
            'command': 'extract',
            'source_type': 'file',
            'source': '/path/to/file.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    ParserCase(
        identifier='load-default-format',
        args=('load', '/path/to/file.json', 'file', '/path/to/output.json'),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.json',
        },
    ),
    ParserCase(
        identifier='load-explicit-format',
        args=(
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.csv',
            '--format',
            'csv',
        ),
        expected={
            'command': 'load',
            'source': '/path/to/file.json',
            'target_type': 'file',
            'target': '/path/to/output.csv',
            'format': 'csv',
            '_format_explicit': True,
        },
    ),
    ParserCase(
        identifier='no-subcommand',
        args=(),
        expected={'command': None},
    ),
    ParserCase(
        identifier='transform',
        args=('transform', '/path/to/file.json'),
        expected={'command': 'transform', 'source': '/path/to/file.json'},
    ),
    ParserCase(
        identifier='validate',
        args=('validate', '/path/to/file.json'),
        expected={'command': 'validate', 'source': '/path/to/file.json'},
    ),
)


@dataclass(slots=True)
class ParserStub:
    """Minimal stand-in for :class:`argparse.ArgumentParser`.

    The production :func:`etlplus.cli.main` only needs a ``parse_args`` method
    returning a namespace.

    Attributes
    ----------
    namespace : argparse.Namespace
        Namespace returned by :meth:`parse_args`.
    """

    namespace: argparse.Namespace

    def parse_args(
        self,
        _args: Sequence[str] | None = None,
    ) -> argparse.Namespace:
        """Return the pre-configured namespace."""
        return self.namespace


class DummyCfg:
    """Minimal stand-in pipeline config for CLI helper tests."""

    name = 'p1'
    version = 'v1'
    sources = [types.SimpleNamespace(name='s1')]
    targets = [types.SimpleNamespace(name='t1')]
    transforms = [types.SimpleNamespace(name='tr1')]
    jobs = [types.SimpleNamespace(name='j1')]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='cli_parser')
def cli_parser_fixture() -> argparse.ArgumentParser:
    """
    Provide a fresh CLI parser per test.

    Returns
    -------
    argparse.ArgumentParser
        Newly constructed parser instance.
    """
    return cli.create_parser()


@pytest.fixture(name='parse_cli')
def parse_cli_fixture(
    cli_parser: argparse.ArgumentParser,
) -> ParseCli:
    """
    Provide a callable that parses argv into a namespace.

    Parameters
    ----------
    cli_parser : argparse.ArgumentParser
        Parser instance created per test.

    Returns
    -------
    ParseCli
        Callable that parses CLI args into an :class:`argparse.Namespace`.
    """

    def _parse(args: Sequence[str]) -> argparse.Namespace:
        return cli_parser.parse_args(list(args))

    return _parse


# SECTION: TESTS ============================================================ #


class TestCliInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli`."""

    def test_add_format_options_sets_defaults(self) -> None:
        """Test that adding format options sets sane defaults."""
        # pylint: disable=protected-access

        parser = argparse.ArgumentParser()
        cli._add_format_options(parser, context='source')
        ns = parser.parse_args([])
        assert hasattr(ns, '_format_explicit')
        assert ns._format_explicit is False

        ns_strict = parser.parse_args(['--strict-format'])
        assert ns_strict.strict_format is True

        ns_format = parser.parse_args(['--format', 'json'])
        assert ns_format.format == 'json'
        assert ns_format._format_explicit is True

    def test_emit_behavioral_notice_error(self) -> None:
        """Test that the helper rejects unknown behaviors."""
        # pylint: disable=protected-access

        with pytest.raises(ValueError):
            cli._emit_behavioral_notice('msg', 'error')

    def test_emit_behavioral_notice_silent(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that silent/ignore behaviors do not emit output."""
        # pylint: disable=protected-access

        cli._emit_behavioral_notice('msg', 'ignore')
        cli._emit_behavioral_notice('msg', 'silent')
        out = capsys.readouterr()
        assert out.err == ''

    def test_emit_behavioral_notice_warn(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that warn behavior writes to stderr."""
        # pylint: disable=protected-access

        cli._emit_behavioral_notice('msg', 'warn')
        out = capsys.readouterr()
        assert 'Warning: msg' in out.err

    def test_format_action_sets_flag(self) -> None:
        """Test that the custom argparse action marks format as explicit."""
        # pylint: disable=protected-access

        parser = argparse.ArgumentParser()
        parser.add_argument('--format', action=cli._FormatAction)
        ns = parser.parse_args(['--format', 'json'])
        assert ns.format == 'json'
        assert ns._format_explicit is True

    def test_format_behavior_strict(self) -> None:
        """Test strict mode behavior mapping."""
        # pylint: disable=protected-access

        assert cli._format_behavior(True) == 'error'

    def test_format_behavior_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test environment-driven behavior mapping when not strict."""
        # pylint: disable=protected-access

        monkeypatch.setenv(cli.FORMAT_ENV_KEY, 'fail')
        assert cli._format_behavior(False) == 'fail'
        monkeypatch.delenv(cli.FORMAT_ENV_KEY, raising=False)
        assert cli._format_behavior(False) == 'warn'

    def test_handle_format_guard_file_format_explicit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that an explicit format triggers guard behavior for files."""
        # pylint: disable=protected-access

        monkeypatch.setattr(cli, '_format_behavior', lambda _strict: 'error')
        with pytest.raises(ValueError):
            cli._handle_format_guard(
                io_context='source',
                resource_type='file',
                format_explicit=True,
                strict=False,
            )

    def test_handle_format_guard_noop_cases(self) -> None:
        """Test guard no-op cases (non-file or not explicit)."""
        # pylint: disable=protected-access

        cli._handle_format_guard(
            io_context='source',
            resource_type='database',
            format_explicit=True,
            strict=False,
        )
        cli._handle_format_guard(
            io_context='source',
            resource_type='file',
            format_explicit=False,
            strict=False,
        )

    def test_list_sections_all(self) -> None:
        """Test that section selection includes all flagged sections."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        result = cli._list_sections(DummyCfg(), args)  # type: ignore[arg-type]
        assert 'pipelines' in result
        assert 'sources' in result
        assert 'targets' in result
        assert 'transforms' in result

    def test_list_sections_default(self) -> None:
        """Test that default selection returns jobs when no flags are set."""
        # pylint: disable=protected-access

        args = argparse.Namespace(
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        result = cli._list_sections(DummyCfg(), args)  # type: ignore[arg-type]
        assert 'jobs' in result

    def test_materialize_csv_payload_csv(self, tmp_path: Path) -> None:
        """Test that CSV file paths are loaded into row dicts."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.csv'
        f.write_text('a,b\n1,2\n3,4\n')
        rows = cli._materialize_csv_payload(str(f))
        assert isinstance(rows, list)
        assert rows[0] == {'a': '1', 'b': '2'}

    def test_materialize_csv_payload_non_csv(self, tmp_path: Path) -> None:
        """Test that non-CSV file paths are returned unchanged."""
        # pylint: disable=protected-access

        f = tmp_path / 'file.txt'
        f.write_text('abc')
        assert cli._materialize_csv_payload(str(f)) == str(f)

    def test_materialize_csv_payload_non_str(self) -> None:
        """Test that non-string payloads are returned unchanged."""
        # pylint: disable=protected-access

        payload: object = {'foo': 1}
        assert cli._materialize_csv_payload(payload) is payload

    def test_pipeline_summary(self) -> None:
        """Test pipeline summary mapping."""
        # pylint: disable=protected-access

        cfg = DummyCfg()
        result = cli._pipeline_summary(cfg)  # type: ignore[arg-type]
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert 'sources' in result
        assert 'targets' in result
        assert 'jobs' in result

    def test_read_csv_rows(self, tmp_path: Path) -> None:
        """Test CSV row reading helper."""
        # pylint: disable=protected-access

        f = tmp_path / 'data.csv'
        f.write_text('a,b\n1,2\n3,4\n')
        rows = cli._read_csv_rows(f)
        assert rows == [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}]

    def test_write_json_output_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test JSON output helper when writing to a file."""
        # pylint: disable=protected-access

        data = {'x': 1}
        outpath = tmp_path / 'out.json'

        class DummyFile:
            last: object | None = None

            def write_json(self, d: object) -> None:
                DummyFile.last = d

        monkeypatch.setattr(cli, 'File', lambda p, f: DummyFile())
        cli._write_json_output(data, str(outpath), success_message='msg')
        assert DummyFile.last == data

    def test_write_json_output_prints(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test JSON output helper when writing to stdout."""
        # pylint: disable=protected-access

        data = {'x': 1}
        assert (
            cli._write_json_output(data, None, success_message='msg') is False
        )
        out = capsys.readouterr()
        assert out.out == ''


class TestCreateParser:
    """
    Unit test suite for :func:`etlplus.cli.create_parser`.

    Notes
    -----
    - Tests CLI parser creation and argument parsing for commands.
    """

    def test_create_parser(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """
        Test that the CLI parser is constructed and reports the CLI tool's
        expected program name.
        """
        assert isinstance(cli_parser, argparse.ArgumentParser)
        assert cli_parser.prog == 'etlplus'

    @pytest.mark.parametrize('case', CLI_CASES, ids=lambda c: c.identifier)
    def test_parser_commands(
        self,
        parse_cli: ParseCli,
        case: ParserCase,
    ) -> None:
        """
        Test CLI command parsing and argument mapping.

        Parameters
        ----------
        parse_cli : ParseCli
            Fixture that parses CLI arguments.
        case : ParserCase
            Declarative parser scenario definition.
        """
        ns = parse_cli(case.args)
        for key, expected in case.expected.items():
            assert getattr(ns, key, None) == expected

    def test_parser_version(
        self,
        cli_parser: argparse.ArgumentParser,
    ) -> None:
        """Test that the CLI parser provides version information."""
        with pytest.raises(SystemExit) as exc_info:
            cli_parser.parse_args(['--version'])
        assert exc_info.value.code == 0

    # def test_parser_includes_expected_subcommands(
    #     self,
    #     cli_parser: argparse.ArgumentParser,
    # ) -> None:
    #     """Test that expected subcommands are registered on the parser."""
    #     # pylint: disable=protected-access

    #     # NOTE: This intentionally inspects argparse internals; it is a
    #     # small, focused smoke test to ensure all expected subcommands are
    #     # present.
    #     # type: ignore[union-attr]
    #     subparsers = cli_parser._subparsers._group_actions[0]
    #     # type: ignore[union-attr]
    #     subcmds = [a.dest for a in subparsers._choices_actions]
    #     assert 'extract' in subcmds
    #     assert 'validate' in subcmds
    #     assert 'transform' in subcmds
    #     assert 'load' in subcmds
    #     assert 'pipeline' in subcmds
    #     assert 'list' in subcmds
    #     assert 'run' in subcmds


class TestMain:
    """Unit test suite for :func:`etlplus.cli.main`."""

    @pytest.mark.parametrize(
        'argv',
        [
            ['extract', 'file', 'foo'],
            ['validate', 'foo'],
            ['transform', 'foo'],
            ['load', 'foo', 'file', 'bar'],
            ['pipeline', '--config', 'foo.yml'],
            ['list', '--config', 'foo.yml'],
            ['run', '--config', 'foo.yml'],
        ],
    )
    def test_dispatches_all_subcommands(
        self,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
    ) -> None:
        """Test that :func:`main` dispatches all subcommands to ``func``."""
        parser = cli.create_parser()
        args = parser.parse_args(argv)
        args.func = lambda _a: 0

        monkeypatch.setattr(cli, 'create_parser', lambda: parser)
        monkeypatch.setattr(parser, 'parse_args', lambda _argv: args)

        assert cli.main(argv) == 0

    def test_handles_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`main` maps keyboard interrupts to exit code 130."""

        def _cmd(*_args: object, **_kwargs: object) -> int:
            raise KeyboardInterrupt

        ns = argparse.Namespace(command='dummy', func=_cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        assert cli.main([]) == 130

    def test_handles_system_exit_from_command(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` does not swallow :class:`SystemExit` from the
        dispatched command.
        """

        def _cmd(*_args: object, **_kwargs: object) -> int:
            raise SystemExit(5)

        ns = argparse.Namespace(command='dummy', func=_cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        with pytest.raises(SystemExit) as exc_info:
            cli.main([])
        assert exc_info.value.code == 5

    def test_invokes_parser(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`main` calls :func:`create_parser` and dispatches to
        the command.
        """
        calls: dict[str, int] = {'parser': 0, 'cmd': 0}

        def _cmd(*_args: object, **_kwargs: object) -> int:
            calls['cmd'] += 1
            return 0

        ns = argparse.Namespace(command='dummy', func=_cmd)

        def _fake_create_parser() -> ParserStub:
            calls['parser'] += 1
            return ParserStub(ns)

        monkeypatch.setattr(cli, 'create_parser', _fake_create_parser)

        assert cli.main([]) == 0
        assert calls['parser'] == 1
        assert calls['cmd'] == 1

    def test_value_error_returns_exit_code_1(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that ValueError from a command maps to exit code 1."""

        def _cmd(*_args: object, **_kwargs: object) -> int:
            raise ValueError('fail')

        ns = argparse.Namespace(command='dummy', func=_cmd)
        monkeypatch.setattr(cli, 'create_parser', lambda: ParserStub(ns))

        assert cli.main([]) == 1
        err = capsys.readouterr().err
        assert 'Error: fail' in err
