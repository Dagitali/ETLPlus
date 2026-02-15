"""
:mod:`tests.unit.meta.test_u_contract_readme` module.

Contract tests for README consistency against runtime registry metadata.
"""

from __future__ import annotations

import re
from pathlib import Path

from etlplus.file import FileFormat
from etlplus.file import registry as mod
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.stub import StubFileHandlerABC

# SECTION: INTERNAL CONSTANTS =============================================== #


_REPO_ROOT = Path(__file__).resolve().parents[2]
_FILE_PACKAGE_README_PATH = _REPO_ROOT / 'etlplus' / 'file' / 'README.md'
_README_MATRIX_PATH = _REPO_ROOT / 'README.md'
_DOCS_MATRIX_PATH = _REPO_ROOT / 'docs' / 'file-handler-matrix.md'
_SUPPORTED_FORMATS_SECTION = '## Supported File Formats'
_SUPPORTED_FORMAT_ROW_PATTERN = re.compile(r'^\| (?P<format>[a-z0-9]+)\s+\|')
_MATRIX_ROW_PATTERN = re.compile(
    r'^\| `(?P<format>[^`]+)` \| `(?P<handler>[^`]+)` \| '
    r'`(?P<base>[^`]+)` \| `(?P<support>[^`]+)` \| '
    r'`(?P<status>[^`]+)` \|$',
)

type MatrixRow = tuple[str, str, str, str]
type MatrixRowsByFormat = dict[FileFormat, MatrixRow]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _matrix_base_abc_name(handler_class: type[FileHandlerABC]) -> str:
    """Return the first category ABC name in one handler class MRO."""
    for base in handler_class.mro()[1:]:
        if base is FileHandlerABC:
            continue
        if not base.__name__.endswith('FileHandlerABC'):
            continue
        first_category_abc = base.__name__
        if first_category_abc == 'ReadOnlyFileHandlerABC' and issubclass(
            handler_class,
            ScientificDatasetFileHandlerABC,
        ):
            if issubclass(
                handler_class,
                SingleDatasetScientificFileHandlerABC,
            ):
                return 'SingleDatasetScientificFileHandlerABC'
            return 'ScientificDatasetFileHandlerABC'
        return first_category_abc
    raise AssertionError(
        f'No category ABC found for handler {handler_class.__name__!r}',
    )


def _matrix_support_text(handler_class: type[FileHandlerABC]) -> str:
    """Return matrix read/write support text for one handler class."""
    supports_read = bool(getattr(handler_class, 'supports_read', True))
    supports_write = bool(getattr(handler_class, 'supports_write', True))
    if supports_read and not supports_write:
        return 'read-only'
    return 'read/write'


def _expected_matrix_row(file_format: FileFormat) -> MatrixRow:
    """Build expected matrix metadata for one mapped format."""
    handler_class = mod.get_handler_class(file_format)
    return (
        handler_class.__name__,
        _matrix_base_abc_name(handler_class),
        _matrix_support_text(handler_class),
        (
            'stub'
            if issubclass(handler_class, StubFileHandlerABC)
            else 'implemented'
        ),
    )


def _expected_supported_formats() -> set[FileFormat]:
    """Return non-stub formats from explicit registry mappings."""
    # pylint: disable=protected-access

    expected: set[FileFormat] = set()
    for file_format in mod._HANDLER_CLASS_SPECS:
        handler_class = mod.get_handler_class(file_format)
        if not issubclass(handler_class, StubFileHandlerABC):
            expected.add(file_format)
    return expected


def _parse_file_package_supported_formats(
    path: Path,
) -> set[FileFormat]:
    """Parse supported-format rows from ``etlplus/file/README.md``."""
    in_supported_section = False
    formats: set[FileFormat] = set()
    for line in path.read_text(encoding='utf-8').splitlines():
        if line.startswith('## '):
            if in_supported_section:
                break
            in_supported_section = line.strip() == _SUPPORTED_FORMATS_SECTION
            continue
        if not in_supported_section:
            continue
        if (match := _SUPPORTED_FORMAT_ROW_PATTERN.match(line)) is None:
            continue
        raw_format = match.group('format')
        if raw_format == 'format':
            continue
        formats.add(FileFormat(raw_format))
    assert formats
    return formats


def _parse_matrix_rows(path: Path) -> MatrixRowsByFormat:
    """Parse handler-matrix rows from one markdown document."""
    rows: MatrixRowsByFormat = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        if (match := _MATRIX_ROW_PATTERN.match(line)) is None:
            continue
        file_format = FileFormat(match.group('format'))
        row: MatrixRow = (
            match.group('handler'),
            match.group('base'),
            match.group('support'),
            match.group('status'),
        )
        assert file_format not in rows
        rows[file_format] = row
    return rows


# SECTION: TESTS ============================================================ #


class TestRegistryDocsMatrixGuardrail:
    """Contract tests for registry/documentation matrix consistency."""

    # pylint: disable=protected-access

    def test_matrix_rows_cover_explicit_registry_mappings(self) -> None:
        """Test both matrix docs covering every explicitly mapped format."""
        expected_formats = set(mod._HANDLER_CLASS_SPECS)
        assert set(_parse_matrix_rows(_README_MATRIX_PATH)) == expected_formats
        assert set(_parse_matrix_rows(_DOCS_MATRIX_PATH)) == expected_formats

    def test_matrix_rows_match_registry_metadata(self) -> None:
        """Test matrix rows matching registry-resolved handler metadata."""
        readme_rows = _parse_matrix_rows(_README_MATRIX_PATH)
        docs_rows = _parse_matrix_rows(_DOCS_MATRIX_PATH)
        for file_format in mod._HANDLER_CLASS_SPECS:
            expected = _expected_matrix_row(file_format)
            assert readme_rows[file_format] == expected
            assert docs_rows[file_format] == expected
            assert readme_rows[file_format] == docs_rows[file_format]


class TestReadmeFileFormatTableGuardrail:
    """Contract tests for ``etlplus/file/README.md`` format table."""

    def test_supported_table_matches_non_stub_registry_mappings(
        self,
    ) -> None:
        """Test supported-format table matching explicit non-stub handlers."""
        documented_formats = _parse_file_package_supported_formats(
            _FILE_PACKAGE_README_PATH,
        )
        assert documented_formats == _expected_supported_formats()
