"""
:mod:`tests.integration.file.test_i_file_smoke` module.

Parameterized integration smoke tests for :mod:`etlplus.file` handlers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.utils._types import JSONDict
from etlplus.utils._types import JSONList

from .conftest import FILE_SMOKE_CASES
from .conftest import FileSmokeCase
from .conftest import run_file_smoke

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'case',
    [pytest.param(case, id=case.id) for case in FILE_SMOKE_CASES],
)
def test_file_handler_roundtrip_smoke(
    case: FileSmokeCase,
    tmp_path: Path,
    sample_record: JSONDict,
    sample_records: JSONList,
) -> None:
    """Test that file handlers can read/write minimal representative payloads."""
    run_file_smoke(
        case.import_module(),
        case.path_for(tmp_path),
        case.payload_for(
            sample_record=sample_record,
            sample_records=sample_records,
        ),
        write_kwargs=case.write_kwargs,
        expect_write_error=case.expect_write_error,
        error_match=case.error_match,
    )
