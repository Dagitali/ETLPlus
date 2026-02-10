"""
:mod:`tests.unit.file.test_u_file_txt` module.

Unit tests for :mod:`etlplus.file.txt`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import txt as mod
from etlplus.types import JSONData
from tests.unit.file.conftest import TextRowModuleContract

# SECTION: TESTS ============================================================ #


class TestTxt(TextRowModuleContract):
    """Unit tests for :mod:`etlplus.file.txt`."""

    # pylint: disable=unused-variable

    module = mod
    format_name = 'txt'
    write_payload = [{'text': 'alpha'}, {'text': 'beta'}]
    expected_written_count = 2

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert TXT write contract output."""
        assert path.read_text(encoding='utf-8') == 'alpha\nbeta\n'

    def prepare_read_case(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> tuple[Path, JSONData]:
        """Prepare representative TXT read input."""
        path = tmp_path / 'data.txt'
        path.write_text('alpha\n\nbeta\n', encoding='utf-8')

        return path, [{'text': 'alpha'}, {'text': 'beta'}]

    def test_write_accepts_single_dict(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` accepts a single dictionary as payload."""
        path = tmp_path / 'data.txt'

        written = mod.write(path, {'text': 'alpha'})

        assert written == 1
        assert path.read_text(encoding='utf-8') == 'alpha\n'

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` returns zero when given an empty payload."""
        path = tmp_path / 'data.txt'
        assert mod.write(path, []) == 0
        assert not path.exists()

    def test_write_rejects_missing_text_key(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` rejects payloads missing the 'text' key."""
        path = tmp_path / 'data.txt'

        with pytest.raises(TypeError, match='TXT payloads must include'):
            mod.write(path, [{'nope': 'value'}])

    def test_write_rejects_non_dict_payloads(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` rejects payloads that are not dicts."""
        path = tmp_path / 'data.txt'

        with pytest.raises(TypeError, match='TXT payloads must contain'):
            mod.write(path, cast(list[dict[str, Any]], [1]))
