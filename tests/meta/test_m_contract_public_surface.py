"""
:mod:`tests.meta.test_m_contract_public_surface` module.

Contract tests for ETLPlus stable CLI and import surfaces.
"""

from __future__ import annotations

import pytest

import etlplus
import etlplus.api as api_pkg
import etlplus.cli as cli_pkg
import etlplus.cli._commands as cli_commands
import etlplus.ops as ops_pkg
from etlplus import Config
from etlplus.api import EndpointClient
from etlplus.api import PaginationConfig
from etlplus.api import PaginationType
from etlplus.api import Paginator
from etlplus.api import RateLimitConfig
from etlplus.api import RateLimiter
from etlplus.api import RetryManager
from etlplus.cli import main as main_mod
from etlplus.ops.extract import extract
from etlplus.ops.load import load
from etlplus.ops.run import run
from etlplus.ops.run import run_pipeline
from etlplus.ops.transform import transform
from etlplus.ops.validate import validate

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKERS ========================================================== #


# Directory-level marker for meta tests.
pytestmark = [pytest.mark.meta, pytest.mark.contract]


# SECTIONS: CONSTANTS ======================================================= #


EXPECTED_CLI_COMMANDS = {
    'check',
    'extract',
    'history',
    'load',
    'log',
    'render',
    'report',
    'run',
    'status',
    'transform',
    'validate',
}

EXPECTED_OPS_EXPORTS = [
    'AggregateName',
    'OperatorName',
    'PipelineStep',
    'extract',
    'load',
    'maybe_validate',
    'run',
    'run_pipeline',
    'transform',
    'validate',
    'ValidationResultDict',
    'ValidationSettings',
]


# SECTION: TESTS ============================================================ #


class TestStableCliSurface:
    """Contract tests for the documented stable CLI command surface."""

    def test_typer_app_exposes_documented_root_commands(self) -> None:
        """Test that the Typer app keeps the documented command set."""
        command_names = {
            command.name
            for command in cli_commands.app.registered_commands
            if command.name is not None
        }
        assert command_names == EXPECTED_CLI_COMMANDS

    def test_cli_package_export_points_to_main_entrypoint(self) -> None:
        """Test that the public CLI package export remains stable."""
        assert cli_pkg.__all__ == ['main']
        assert cli_pkg.main is main_mod


class TestStableImportSurface:
    """Contract tests for the documented stable Python import surface."""

    def test_top_level_package_keeps_documented_exports(self) -> None:
        """Test that the top-level package keeps stable facade symbols."""
        assert etlplus.__all__ == ['__author__', '__version__', 'Config']
        assert etlplus.Config is Config
        assert isinstance(etlplus.__version__, str)
        assert etlplus.__version__

    def test_ops_package_keeps_documented_entrypoints(self) -> None:
        """Test that :mod:`etlplus.ops` keeps the documented helpers."""
        assert ops_pkg.__all__ == EXPECTED_OPS_EXPORTS
        assert ops_pkg.extract is extract
        assert ops_pkg.load is load
        assert ops_pkg.run is run
        assert ops_pkg.run_pipeline is run_pipeline
        assert ops_pkg.transform is transform
        assert ops_pkg.validate is validate

    def test_api_package_keeps_documented_core_exports(self) -> None:
        """Test that :mod:`etlplus.api` keeps core documented imports."""
        expected_members = {
            'EndpointClient': EndpointClient,
            'PaginationConfig': PaginationConfig,
            'PaginationType': PaginationType,
            'Paginator': Paginator,
            'RateLimitConfig': RateLimitConfig,
            'RateLimiter': RateLimiter,
            'RetryManager': RetryManager,
        }
        for name, expected in expected_members.items():
            assert name in api_pkg.__all__
            assert getattr(api_pkg, name) is expected
