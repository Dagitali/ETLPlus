"""
tests.integration.test_cli integration tests module.

End-to-end CLI integration tests.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from etlplus.cli import main


# SECTION: TESTS ============================================================ #


class TestCliEndToEnd:
    def test_main_no_command(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(sys, 'argv', ['etlplus'])
        result = main()
        assert result == 0
        captured = capsys.readouterr()
        assert 'usage:' in captured.out.lower()

    def test_main_extract_file(self, monkeypatch, capsys) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False,
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

    def test_main_validate_data(self, monkeypatch, capsys) -> None:
        json_data = '{"name": "John", "age": 30}'
        monkeypatch.setattr(sys, 'argv', ['etlplus', 'validate', json_data])
        result = main()
        assert result == 0
        output = json.loads(capsys.readouterr().out)
        assert output['valid'] is True

    def test_main_transform_data(self, monkeypatch, capsys) -> None:
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

    def test_main_load_file(self, monkeypatch) -> None:
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

    def test_main_extract_with_output(self, monkeypatch) -> None:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False,
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
                        'etlplus', 'extract', 'file', temp_path,
                        '-o', str(output_path),
                    ],
                )
                result = main()
                assert result == 0 and output_path.exists()
                loaded = json.loads(output_path.read_text())
                assert loaded == test_data
            finally:
                Path(temp_path).unlink()

    def test_main_error_handling(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(
            sys,
            'argv',
            ['etlplus', 'extract', 'file', '/nonexistent/file.json'],
        )
        result = main()
        assert result == 1
        captured = capsys.readouterr()
        assert 'Error:' in captured.err
