"""
:mod:`tests.meta.test_m_integration_file_conventions` module.

Guardrails for integration file-smoke contract conventions.
"""

from __future__ import annotations

import pytest

from etlplus.file import FileFormat
from etlplus.file import _registry as mod
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.stub import StubFileHandlerABC
from tests.integration.file.pytest_smoke_file_contracts import FILE_SMOKE_CASES
from tests.integration.file.pytest_smoke_file_contracts import (
    FILE_SMOKE_EXCEPTION_CASES,
)
from tests.integration.file.pytest_smoke_file_contracts import (
    FILE_SMOKE_EXCEPTION_OVERRIDES,
)
from tests.integration.file.pytest_smoke_file_contracts import FileSmokeCase

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestIntegrationFileSmokeConventions:
    """Guardrails for integration file-smoke contract symmetry."""

    def test_file_format_names_match_smoke_case_names(self) -> None:
        """Test that smoke case names stay aligned with ``FileFormat`` values."""
        assert {case.module_name for case in FILE_SMOKE_CASES} <= {
            file_format.value for file_format in FileFormat
        }

    @pytest.mark.parametrize(
        'case',
        [pytest.param(case, id=case.id) for case in FILE_SMOKE_CASES],
    )
    def test_only_documented_cases_use_structural_overrides(
        self,
        case: FileSmokeCase,
    ) -> None:
        """Test that structural path/error overrides stay documented."""
        attrs = case.override_attrs()
        if case.module_name in FILE_SMOKE_EXCEPTION_CASES:
            assert attrs == FILE_SMOKE_EXCEPTION_OVERRIDES[case.module_name]
        else:
            assert not attrs

    def test_read_only_handlers_expect_write_errors(self) -> None:
        """Test that read-only smoke cases assert the expected write failure."""
        read_only_formats = {
            file_format.value
            for file_format in mod._HANDLER_CLASS_SPECS
            if issubclass(mod.get_handler_class(file_format), ReadOnlyFileHandlerABC)
            and not issubclass(mod.get_handler_class(file_format), StubFileHandlerABC)
        }
        observed = {
            case.module_name
            for case in FILE_SMOKE_CASES
            if case.expect_write_error is not None
        }

        assert observed == read_only_formats
