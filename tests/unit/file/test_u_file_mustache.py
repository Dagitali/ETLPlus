"""
:mod:`tests.unit.file.test_u_file_mustache` module.

Unit tests for :mod:`etlplus.file.mustache`.
"""

from __future__ import annotations

from etlplus.file import mustache as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contract_mixins import TemplateFileContractMixin
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestMustache(TemplateFileContractMixin, RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.mustache`."""

    module = mod
    format_name = 'mustache'
    sample_template_text = 'Hello {{name}}'
    roundtrip_spec = build_roundtrip_spec(
        {'template': 'Hi {{name}}'},
        [{'template': 'Hi {{name}}'}],
    )

    def test_render_substitutes_mustache_tokens(self) -> None:
        """Test that :meth:`render` replaces simple Mustache variables."""
        result = self.module_handler.render(
            'Hello {{name}} from {{city}}.',
            {'name': 'Ada', 'city': 'Paris'},
        )

        assert result == 'Hello Ada from Paris.'
