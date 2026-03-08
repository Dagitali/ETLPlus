"""
:mod:`tests.unit.file.test_u_file_vm` module.

Unit tests for :mod:`etlplus.file.vm`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import vm as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestVm(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.vm`."""

    module = mod
    format_name = 'vm'
    roundtrip_spec = build_roundtrip_spec(
        {'template': 'Hi $name'},
        [{'template': 'Hi $name'}],
    )

    def test_read_returns_template_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :meth:`read` returns one-row payload with template text.
        """
        path = self.format_path(tmp_path)
        path.write_text('Hello $name', encoding='utf-8')

        assert self.module_handler.read(path) == [{'template': 'Hello $name'}]

    def test_render_substitutes_velocity_tokens(self) -> None:
        """
        Test that :meth:`render` replaces plain and braced Velocity variables.
        """
        result = self.module_handler.render(
            'Hello $name from ${city}.',
            {'name': 'Ada', 'city': 'Paris'},
        )

        assert result == 'Hello Ada from Paris.'

    def test_write_requires_single_template_object(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :meth:`write` requires one payload object with a template
        string.
        """
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match='exactly one object'):
            self.module_handler.write(
                path,
                [{'template': 'a'}, {'template': 'b'}],
            )
        with pytest.raises(TypeError, match='"template" string'):
            self.module_handler.write(path, [{'name': 'missing'}])

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that empty write payload returns 0 without creating file."""
        path = self.format_path(tmp_path)

        assert self.module_handler.write(path, []) == 0
        assert not path.exists()
