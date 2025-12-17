"""
:mod:`tests.integration.test_i_cli` module.

End-to-end CLI integration test suite. Exercises the ``etlplus`` command for
core subcommands without external dependencies by operating on temporary files
and in-memory data.

Notes
-----
- Verifies usage output when no command is provided.
- Tests extract/validate/transform/load flows via CLI arguments.
- Uses ``tempfile`` and ``pathlib.Path`` for filesystem isolation.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from pytest import CaptureFixture
from pytest import MonkeyPatch

from etlplus.cli import main

# SECTION: TESTS ============================================================ #


class TestCliEndToEnd:
    """Integration test suite for :mod:`etlplus.cli`."""

    def test_extract_format_error_strict_flag(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that :func:`extract` with ``--strict-format`` and incorrect format
        errors.
        """
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
        ) as f:
            json.dump({'x': 1}, f)
            temp_path = f.name
        try:
            monkeypatch.setattr(
                sys,
                'argv',
                [
                    'etlplus',
                    'extract',
                    'file',
                    temp_path,
                    '--format',
                    'json',
                    '--strict-format',
                ],
            )
            result = main()
            assert result == 1
            captured = capsys.readouterr()
            assert 'Error:' in captured.err
        finally:
            Path(temp_path).unlink()

    def test_extract_format_warns_default(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that :func:`extract` with default format and incorrect format
        warns.
        """
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
        ) as f:
            json.dump({'x': 1}, f)
            temp_path = f.name
        try:
            monkeypatch.setattr(
                sys,
                'argv',
                [
                    'etlplus',
                    'extract',
                    'file',
                    temp_path,
                    '--format',
                    'json',
                ],
            )
            result = main()
            assert result == 0
            captured = capsys.readouterr()
            assert 'Warning:' in captured.err
        finally:
            Path(temp_path).unlink()

    def test_load_format_error_strict_flag(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that :func:`load` with ``--strict-format`` and incorrect format
        errors.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.csv'
            json_data = '{"name": "John"}'
            monkeypatch.setattr(
                sys,
                'argv',
                [
                    'etlplus',
                    'load',
                    json_data,
                    'file',
                    str(output_path),
                    '--format',
                    'csv',
                    '--strict-format',
                ],
            )
            result = main()
            assert result == 1
            captured = capsys.readouterr()
            assert 'Error:' in captured.err
            assert not output_path.exists()

    def test_load_format_warns_default(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that `:func:`load` with default format and incorrect format warns.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.csv'
            json_data = '{"name": "John"}'
            monkeypatch.setattr(
                sys,
                'argv',
                [
                    'etlplus',
                    'load',
                    json_data,
                    'file',
                    str(output_path),
                    '--format',
                    'csv',
                ],
            )
            result = main()
            assert result == 0
            captured = capsys.readouterr()
            assert 'Warning:' in captured.err
            assert output_path.exists()

    def test_main_no_command(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with no command shows usage.
        """
        monkeypatch.setattr(sys, 'argv', ['etlplus'])
        result = main()
        assert result == 0
        captured = capsys.readouterr()
        assert 'usage:' in captured.out.lower()

    def test_main_extract_file(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with the ``extract`` file command works.
        """
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
        ) as f:
            test_data = {'name': 'John', 'age': 30}
            json.dump(test_data, f)
            temp_path = f.name
        try:
            monkeypatch.setattr(
                sys,
                'argv',
                ['etlplus', 'extract', 'file', temp_path],
            )
            result = main()
            assert result == 0
            captured = capsys.readouterr()
            output_data = json.loads(captured.out)
            assert output_data == test_data
        finally:
            Path(temp_path).unlink()

    def test_main_validate_data(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with the ``validate`` command works.
        """
        json_data = '{"name": "John", "age": 30}'
        monkeypatch.setattr(sys, 'argv', ['etlplus', 'validate', json_data])
        result = main()
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output['valid'] is True

    def test_main_transform_data(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with the ``transform`` command works.
        """
        json_data = '[{"name": "John", "age": 30}]'
        operations = '{"select": ["name"]}'
        monkeypatch.setattr(
            sys,
            'argv',
            ['etlplus', 'transform', json_data, '--operations', operations],
        )
        result = main()
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert len(output) == 1 and 'age' not in output[0]

    def test_main_load_file(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """
        Test that running :func:`main` with the ``load`` file command works.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.json'
            json_data = '{"name": "John", "age": 30}'
            monkeypatch.setattr(
                sys,
                'argv',
                ['etlplus', 'load', json_data, 'file', str(output_path)],
            )
            result = main()
            assert result == 0
            assert output_path.exists()

    def test_main_extract_with_output(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """
        Test that running :func:`main` with the ``extract`` file command and
        ``output`` option works.
        """
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            delete=False,
        ) as f:
            test_data = {'name': 'John', 'age': 30}
            json.dump(test_data, f)
            temp_path = f.name
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.json'
            try:
                monkeypatch.setattr(
                    sys,
                    'argv',
                    [
                        'etlplus',
                        'extract',
                        'file',
                        temp_path,
                        '-o',
                        str(output_path),
                    ],
                )
                result = main()
                assert result == 0 and output_path.exists()
                loaded = json.loads(output_path.read_text())
                assert loaded == test_data
            finally:
                Path(temp_path).unlink()

    def test_main_error_handling(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with an invalid command errors.
        """
        monkeypatch.setattr(
            sys,
            'argv',
            ['etlplus', 'extract', 'file', '/nonexistent/file.json'],
        )
        result = main()
        assert result == 1
        captured = capsys.readouterr()
        assert 'Error:' in captured.err

    def test_main_strict_format_error(
        self,
        monkeypatch: MonkeyPatch,
        capsys: CaptureFixture[str],
    ) -> None:
        """
        Test that running :func:`main` with the ``extract`` file command and
        ``--strict-format`` option with an incorrect format errors.
        """
        # Passing --format for a file with --strict-format should error
        monkeypatch.setattr(
            sys,
            'argv',
            [
                'etlplus',
                'extract',
                'file',
                'data.csv',
                '--format',
                'csv',
                '--strict-format',
            ],
        )
        result = main()
        assert result == 1
        captured = capsys.readouterr()
        assert 'Error:' in captured.err
