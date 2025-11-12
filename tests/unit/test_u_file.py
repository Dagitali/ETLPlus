"""
``tests.unit.test_u_file`` module.

Unit tests for the ETLPlus file handling helpers.

Notes
-----
These tests cover file format inference from extensions and
error handling for unknown formats.
"""
from pathlib import Path

from etlplus.enums import FileFormat
from etlplus.file import File


# SECTION: TESTS =========================================================== #


def test_infers_json_from_extension(tmp_path: Path):
    """
    Test JSON file inference from extension.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path.
    """

    p = tmp_path / 'data.json'
    p.write_text('{}', encoding='utf-8')
    f = File(p)
    assert f.file_format == FileFormat.JSON
    assert f.read() == {}


def test_unknown_extension_defers_error(tmp_path: Path):
    """
    Test unknown file extension handling.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path.
    """

    p = tmp_path / 'weird.data'
    p.write_text('{}', encoding='utf-8')
    f = File(p)
    assert f.file_format is None
    try:
        f.read()
    except ValueError as e:
        assert 'Cannot infer file format' in str(e)
