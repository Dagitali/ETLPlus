"""
:mod:`tests.unit.cli.test_u_cli_state` module.

Unit tests for :mod:`etlplus.cli.state`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

import etlplus
import etlplus.cli.handlers as handlers
import etlplus.cli.state as cli_state_module
from tests.unit.cli.conftest import CaptureHandler
from tests.unit.cli.conftest import InvokeCli
from tests.unit.cli.conftest import assert_mapping_contains

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestCliExtractState:
    """Unit test suite of command-line state tests for ``extract``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        (
            pytest.param(
                ('extract', '/path/to/file.csv', '--source-format', 'csv'),
                {
                    'source': '/path/to/file.csv',
                    'format_hint': 'csv',
                    'format_explicit': True,
                },
                id='extract-file-format',
            ),
            pytest.param(
                (
                    '--no-pretty',
                    '--quiet',
                    'extract',
                    '--source-type',
                    'api',
                    'https://example.com/data.json',
                ),
                {
                    'source_type': 'api',
                    'source': 'https://example.com/data.json',
                    'pretty': False,
                },
                id='extract-api-quiet',
            ),
        ),
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handlers, 'extract_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliLoadState:
    """Unit test suite of command-line state tests for ``load``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        (
            pytest.param(
                ('load', '--target-type', 'file', '/path/to/out.json'),
                {
                    'target': '/path/to/out.json',
                    'target_format': None,
                    'format_explicit': False,
                },
                id='load-file-target',
            ),
            pytest.param(
                ('load', '--target-format', 'csv', '/path/to/out.csv'),
                {'target_format': 'csv', 'format_explicit': True},
                id='load-explicit-target-format',
            ),
            pytest.param(
                (
                    'load',
                    '--target-type',
                    'database',
                    'postgres://db.example.org/app',
                ),
                {'source': '-', 'target': 'postgres://db.example.org/app'},
                id='load-default-source',
            ),
        ),
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handlers, 'load_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliRenderState:
    """Unit test suite of command-line state tests for ``render``."""

    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handlers, 'render_handler')

        result = invoke_cli(
            'render',
            '--config',
            'pipeline.yml',
            '--table',
            'Customers',
            '--template',
            'ddl',
            '--output',
            'out.sql',
        )

        assert result.exit_code == 0
        assert calls['config'] == 'pipeline.yml'
        assert calls['table'] == 'Customers'
        assert calls['template'] == 'ddl'
        assert calls['output'] == 'out.sql'


class TestCliRunState:
    """Unit test suite of command-line state tests for ``run``."""

    def test_maps_flags(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
    ) -> None:
        """Test that CLI flags map to handler parameters correctly."""
        calls = capture_handler(handlers, 'run_handler')

        result = invoke_cli('run', '--config', 'p.yml', '--job', 'j1')

        assert result.exit_code == 0
        assert calls['config'] == 'p.yml'
        assert calls['job'] == 'j1'


class TestCliTransformState:
    """Unit test suite of command-line state tests for ``transform``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        (
            pytest.param(
                (
                    'transform',
                    '/path/to/file.json',
                    '--operations',
                    '{"select": ["id"]}',
                ),
                {
                    'source': '/path/to/file.json',
                    'operations': {'select': ['id']},
                },
                id='transform-inline-ops',
            ),
            pytest.param(
                ('transform', '--source-format', 'csv'),
                {'source_format': 'csv'},
                id='transform-source-format',
            ),
        ),
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handlers, 'transform_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliValidateState:
    """Unit test suite of command-line state tests for ``validate``."""

    @pytest.mark.parametrize(
        ('argv', 'expected'),
        (
            pytest.param(
                (
                    'validate',
                    '/path/to/file.json',
                    '--rules',
                    '{"required": ["id"]}',
                ),
                {
                    'source': '/path/to/file.json',
                    'rules': {'required': ['id']},
                },
                id='validate-inline-rules',
            ),
            pytest.param(
                ('validate', '--source-format', 'csv'),
                {'source_format': 'csv'},
                id='validate-source-format',
            ),
        ),
    )
    def test_maps_namespace(
        self,
        invoke_cli: InvokeCli,
        capture_handler: CaptureHandler,
        argv: tuple[str, ...],
        expected: dict[str, object],
    ) -> None:
        """Test that CLI args map to handler parameters correctly."""
        calls = capture_handler(handlers, 'validate_handler')

        result = invoke_cli(*argv)

        assert result.exit_code == 0
        assert_mapping_contains(calls, expected)


class TestCliHelp:
    """Unit test suite of command-line state tests for help text."""

    def test_no_args_prints_help(self, invoke_cli: InvokeCli) -> None:
        """Test that running with no arguments prints help text."""
        result = invoke_cli()
        assert result.exit_code == 0
        assert 'ETLPlus' in result.stdout


class TestCliVersionFlag:
    """Unit test suite of command-line state tests for global flags."""

    def test_version_flag_exits_zero(self, invoke_cli: InvokeCli) -> None:
        """Test that command option ``--version`` exits successfully."""
        result = invoke_cli('--version')
        assert result.exit_code == 0
        assert etlplus.__version__ in result.stdout


class TestInferResourceType:
    """Unit test suite for :func:`infer_resource_type`."""

    def test_file_path(self, tmp_path: Path) -> None:
        """
        Test that :func:`infer_resource_type` detects local files via
        extension parsing.
        """
        path = tmp_path / 'payload.csv'
        path.write_text('a,b\n1,2\n', encoding='utf-8')
        assert cli_state_module.infer_resource_type(str(path)) == 'file'

    def test_invalid_raises(self) -> None:
        """
        Unknown resources raise ``ValueError`` to surface helpful guidance.
        """
        with pytest.raises(ValueError):
            cli_state_module.infer_resource_type('unknown-resource')

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        (
            ('-', 'file'),
            ('https://example.com/data.json', 'api'),
            ('postgres://user@host/db', 'database'),
        ),
    )
    def test_variants(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """
        Test that :func:`infer_resource_type` classifies common resource
        inputs.
        """
        assert cli_state_module.infer_resource_type(raw) == expected


class TestOptionalChoice:
    """Unit test suite for :func:`optional_choice`."""

    @pytest.mark.parametrize(
        ('choice', 'expected'),
        ((None, None), ('json', 'json')),
    )
    def test_passthrough_and_validation(
        self,
        choice: str | None,
        expected: str | None,
    ) -> None:
        """
        Test that :func:`optional_choice` preserves ``None`` and normalizes
        valid values.
        """
        assert (
            cli_state_module.optional_choice(
                choice,
                {'json', 'csv'},
                label='format',
            )
            == expected
        )

    @pytest.mark.parametrize('invalid', ('yaml', 'parquet'))
    def test_rejects_invalid(self, invalid: str) -> None:
        """Test that invalid choices raise :class:`typer.BadParameter`."""
        with pytest.raises(typer.BadParameter):
            cli_state_module.optional_choice(invalid, {'json'}, label='format')
