"""
:mod:`tests.unit.file.test_u_file_vm` module.

Unit tests for :mod:`etlplus.file.vm`.
"""

from __future__ import annotations

from etlplus.file import vm as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contract_mixins import TemplateFileContractMixin
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestVm(TemplateFileContractMixin, RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.vm`."""

    module = mod
    format_name = 'vm'
    sample_template_text = 'Hello $name'
    roundtrip_spec = build_roundtrip_spec(
        {'template': 'Hi $name'},
        [{'template': 'Hi $name'}],
    )

    def test_render_substitutes_velocity_tokens(self) -> None:
        """
        Test that :meth:`render` replaces plain and braced Velocity variables.
        """
        result = self.module_handler.render(
            'Hello $name from ${city}.',
            {'name': 'Ada', 'city': 'Paris'},
        )

        assert result == 'Hello Ada from Paris.'
