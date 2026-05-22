"""
:mod:`tests.unit.file.test_u_file_hbs` module.

Unit tests for :mod:`etlplus.file.hbs`.
"""

from __future__ import annotations

from etlplus.file import hbs as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contract_mixins import TemplateFileContractMixin
from .pytest_file_contract_mixins import TemplateRenderContractMixin
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHbs(
    TemplateRenderContractMixin,
    TemplateFileContractMixin,
    RoundtripUnitModuleContract,
):
    """Unit tests for :mod:`etlplus.file.hbs`."""

    module = mod
    format_name = 'hbs'
    sample_template_text = 'Hello {{name}}'
    render_template = 'Hello {{name}} from {{city}}.'
    roundtrip_spec = build_roundtrip_spec(
        {'template': 'Hi {{name}}'},
        [{'template': 'Hi {{name}}'}],
    )
