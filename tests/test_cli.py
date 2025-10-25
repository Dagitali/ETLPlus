"""Tests for CLI module."""
import json
import sys
import tempfile
from pathlib import Path

import pytest

from etlplus.__main__ import create_parser
from etlplus.__main__ import main


def test_create_parser():
    """Test parser creation."""
    parser = create_parser()
    assert parser is not None
    assert parser.prog == 'etlplus'


def test_parser_version(capsys):
    """Test version argument."""
    parser = create_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(['--version'])
    assert exc_info.value.code == 0


def test_parser_no_command():
    """Test parser with no command."""
    parser = create_parser()
    args = parser.parse_args([])
    assert args.command is None


def test_parser_extract_command():
    """Test extract command parsing."""
    parser = create_parser()
    args = parser.parse_args(
        ['extract', 'file', '/path/to/file.json'],
    )
    assert args.command == 'extract'
    assert args.source_type == 'file'
    assert args.source == '/path/to/file.json'
    assert args.format == 'json'


def test_parser_validate_command():
    """Test validate command parsing."""
    parser = create_parser()
    args = parser.parse_args(['validate', '/path/to/file.json'])
    assert args.command == 'validate'
    assert args.source == '/path/to/file.json'


def test_parser_transform_command():
    """Test transform command parsing."""
    parser = create_parser()
    args = parser.parse_args(['transform', '/path/to/file.json'])
    assert args.command == 'transform'
    assert args.source == '/path/to/file.json'


def test_parser_load_command():
    """Test load command parsing."""
    parser = create_parser()
    args = parser.parse_args(
        [
            'load',
            '/path/to/file.json',
            'file',
            '/path/to/output.json',
        ],
    )
    assert args.command == 'load'
    assert args.source == '/path/to/file.json'
    assert args.target_type == 'file'
    assert args.target == '/path/to/output.json'


def test_main_no_command(monkeypatch, capsys):
    """Test main with no command."""
    monkeypatch.setattr(sys, 'argv', ['etlplus'])
    result = main()
    assert result == 0
    captured = capsys.readouterr()
    assert 'usage:' in captured.out.lower()


def test_main_extract_file(monkeypatch, capsys):
    """Test main with extract file command."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False,
    ) as f:
        test_data = {'name': 'John', 'age': 30}
        json.dump(test_data, f)
        temp_path = f.name

    try:
        monkeypatch.setattr(
            sys, 'argv', ['etlplus', 'extract', 'file', temp_path],
        )
        result = main()
        assert result == 0
        captured = capsys.readouterr()
        output_data = json.loads(captured.out)
        assert output_data == test_data
    finally:
        Path(temp_path).unlink()


def test_main_validate_data(monkeypatch, capsys):
    """Test main with validate command."""
    json_data = '{"name": "John", "age": 30}'
    monkeypatch.setattr(sys, 'argv', ['etlplus', 'validate', json_data])
    result = main()
    assert result == 0
    captured = capsys.readouterr()
    output_data = json.loads(captured.out)
    assert output_data['valid'] is True


def test_main_transform_data(monkeypatch, capsys):
    """Test main with transform command."""
    json_data = '[{"name": "John", "age": 30}]'
    operations = '{"select": ["name"]}'
    monkeypatch.setattr(
        sys, 'argv', [
            'etlplus', 'transform',
            json_data, '--operations', operations,
        ],
    )
    result = main()
    assert result == 0
    captured = capsys.readouterr()
    output_data = json.loads(captured.out)
    assert len(output_data) == 1
    assert 'age' not in output_data[0]


def test_main_load_file(monkeypatch, capsys):
    """Test main with load file command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / 'output.json'
        json_data = '{"name": "John", "age": 30}'

        monkeypatch.setattr(
            sys, 'argv', [
                'etlplus', 'load', json_data, 'file', str(output_path),
            ],
        )
        result = main()
        assert result == 0
        assert output_path.exists()


def test_main_extract_with_output(monkeypatch, capsys):
    """Test main with extract and output file."""
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
                sys, 'argv', [
                    'etlplus', 'extract', 'file', temp_path, '-o', str(
                        output_path,
                    ),
                ],
            )
            result = main()
            assert result == 0
            assert output_path.exists()

            with open(output_path) as f:
                loaded_data = json.load(f)
            assert loaded_data == test_data
        finally:
            Path(temp_path).unlink()


def test_main_error_handling(monkeypatch, capsys):
    """Test main with error."""
    monkeypatch.setattr(
        sys, 'argv', ['etlplus', 'extract', 'file', '/nonexistent/file.json'],
    )
    result = main()
    assert result == 1
    captured = capsys.readouterr()
    assert 'Error:' in captured.err
