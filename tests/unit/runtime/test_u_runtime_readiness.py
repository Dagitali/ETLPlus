"""
:mod:`tests.unit.test_u_runtime_readiness` module.

Unit tests for :mod:`etlplus.runtime.readiness`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime.readiness as readiness_module

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilder:
    """Unit tests for :class:`ReadinessReportBuilder`."""

    def test_build_matches_wrapper_runtime_only(self) -> None:
        """Test that the class builder matches the function wrapper."""
        expected = readiness_module.build_readiness_report(env={})
        actual = readiness_module.ReadinessReportBuilder.build(env={})

        assert actual == expected

    def test_missing_requirement_rows_respects_package_available_seam(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that class-based dependency checks still honor wrapper patches."""
        monkeypatch.setattr(
            readiness_module,
            '_package_available',
            lambda module_name: False if module_name == 'boto3' else True,
        )
        cfg = SimpleNamespace(
            sources=[
                SimpleNamespace(
                    format='csv',
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
            ],
            targets=[],
            apis={},
        )

        rows = readiness_module.ReadinessReportBuilder.missing_requirement_rows(
            cfg=cast(Any, cfg),
        )

        assert rows == [
            {
                'connector': 's3-source',
                'extra': 'storage',
                'missing_package': 'boto3',
                'reason': 's3 storage path requires boto3',
                'role': 'source',
            },
        ]
