"""
:mod:`tests.meta.test_m_contract_public_surface` module.

Contract tests for ETLPlus stable CLI and import surfaces.
"""

from __future__ import annotations

from typing import Any

import pytest

import etlplus
import etlplus.api as api_pkg
import etlplus.cli as cli_pkg
import etlplus.cli._commands as commands_mod
import etlplus.file as file_pkg
import etlplus.history as history_pkg
import etlplus.ops as ops_pkg
import etlplus.ops.transformations.aggregate as aggregate_tx_mod
import etlplus.ops.transformations.filter as filter_tx_mod
import etlplus.ops.transformations.map as map_tx_mod
import etlplus.ops.transformations.select as select_tx_mod
import etlplus.ops.transformations.sort as sort_tx_mod
from etlplus import Config
from etlplus.api import EndpointClient
from etlplus.api import PaginationConfig
from etlplus.api import PaginationInput
from etlplus.api import PaginationType
from etlplus.api import Paginator
from etlplus.api import RateLimitConfig
from etlplus.api import RateLimiter
from etlplus.api import RateLimitOverrides
from etlplus.api import RetryManager
from etlplus.cli import main as main_mod
from etlplus.file import BoundFileHandler
from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.file import ReadOptions
from etlplus.file import WriteOptions
from etlplus.history import HISTORY_SCHEMA_VERSION
from etlplus.ops.extract import extract
from etlplus.ops.load import load
from etlplus.ops.run import run
from etlplus.ops.run import run_pipeline
from etlplus.ops.transform import transform
from etlplus.ops.transformations.aggregate import apply_aggregate
from etlplus.ops.transformations.aggregate import apply_aggregate_step
from etlplus.ops.transformations.filter import apply_filter
from etlplus.ops.transformations.filter import apply_filter_step
from etlplus.ops.transformations.map import apply_map
from etlplus.ops.transformations.map import apply_map_step
from etlplus.ops.transformations.select import apply_select
from etlplus.ops.transformations.select import apply_select_step
from etlplus.ops.transformations.select import is_plain_fields_list
from etlplus.ops.transformations.select import is_sequence_not_text
from etlplus.ops.transformations.sort import apply_sort
from etlplus.ops.transformations.sort import apply_sort_step
from etlplus.ops.validate import FieldRulesDict
from etlplus.ops.validate import FieldValidationDict
from etlplus.ops.validate import ValidationDict
from etlplus.ops.validate import validate
from etlplus.ops.validate import validate_schema

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
    'init',
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
    'ValidationSettings',
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
    'validate_schema',
    'FieldRulesDict',
    'FieldValidationDict',
    'ValidationDict',
    'ValidationResultDict',
]


# SECTION: TESTS ============================================================ #


class TestStableCliSurface:
    """Contract tests for the documented stable CLI command surface."""

    def test_typer_app_exposes_documented_root_commands(self) -> None:
        """Test that the Typer app keeps the documented command set."""
        command_names = {
            command.name
            for command in commands_mod.app.registered_commands
            if command.name is not None
        }
        assert command_names == EXPECTED_CLI_COMMANDS

    def test_cli_package_export_points_to_main_entrypoint(self) -> None:
        """Test that the public CLI package export remains stable."""
        assert cli_pkg.__all__ == ['main']
        assert cli_pkg.main is main_mod


class TestStableImportSurface:
    """Contract tests for the documented stable Python import surface."""

    def test_api_package_keeps_documented_core_exports(self) -> None:
        """Test that :mod:`etlplus.api` keeps core documented imports."""
        expected_members = {
            'EndpointClient': EndpointClient,
            'PaginationConfig': PaginationConfig,
            'PaginationInput': PaginationInput,
            'PaginationType': PaginationType,
            'Paginator': Paginator,
            'RateLimitConfig': RateLimitConfig,
            'RateLimitOverrides': RateLimitOverrides,
            'RateLimiter': RateLimiter,
            'RetryManager': RetryManager,
        }
        for name, expected in expected_members.items():
            assert name in api_pkg.__all__
            assert getattr(api_pkg, name) is expected

    def test_file_package_keeps_handler_authoring_facade(self) -> None:
        """Test that :mod:`etlplus.file` exposes the handler authoring layer."""
        expected_members = {
            'BoundFileHandler': BoundFileHandler,
            'File': File,
            'FileFormat': FileFormat,
            'ReadOptions': ReadOptions,
            'WriteOptions': WriteOptions,
        }
        for name, expected in expected_members.items():
            assert name in file_pkg.__all__
            assert getattr(file_pkg, name) is expected

    def test_history_package_keeps_documented_runtime_metadata(self) -> None:
        """Test that :mod:`etlplus.history` keeps schema metadata exports."""
        assert 'HISTORY_SCHEMA_VERSION' in history_pkg.__all__
        assert history_pkg.HISTORY_SCHEMA_VERSION == HISTORY_SCHEMA_VERSION

    def test_ops_package_keeps_documented_entrypoints(self) -> None:
        """Test that :mod:`etlplus.ops` keeps the documented helpers."""
        assert ops_pkg.__all__ == EXPECTED_OPS_EXPORTS
        assert ops_pkg.extract is extract
        assert ops_pkg.load is load
        assert ops_pkg.run is run
        assert ops_pkg.run_pipeline is run_pipeline
        assert ops_pkg.transform is transform
        assert ops_pkg.validate is validate
        assert ops_pkg.validate_schema is validate_schema

    def test_ops_package_keeps_documented_validation_shapes(self) -> None:
        """Test that :mod:`etlplus.ops` re-exports validation typed dicts."""
        assert ops_pkg.FieldRulesDict is FieldRulesDict
        assert ops_pkg.FieldValidationDict is FieldValidationDict
        assert ops_pkg.ValidationDict is ValidationDict

    def test_ops_transformations_modules_keep_documented_helpers(self) -> None:
        """Test that step-level transform modules keep documented helpers."""
        expected_members: dict[Any, dict[str, object]] = {
            aggregate_tx_mod: {
                'apply_aggregate': apply_aggregate,
                'apply_aggregate_step': apply_aggregate_step,
            },
            filter_tx_mod: {
                'apply_filter': apply_filter,
                'apply_filter_step': apply_filter_step,
            },
            map_tx_mod: {
                'apply_map': apply_map,
                'apply_map_step': apply_map_step,
            },
            select_tx_mod: {
                'apply_select': apply_select,
                'apply_select_step': apply_select_step,
                'is_plain_fields_list': is_plain_fields_list,
                'is_sequence_not_text': is_sequence_not_text,
            },
            sort_tx_mod: {
                'apply_sort': apply_sort,
                'apply_sort_step': apply_sort_step,
            },
        }
        for module, members in expected_members.items():
            for name, expected in members.items():
                assert name in module.__all__
                assert getattr(module, name) is expected

    def test_top_level_package_keeps_documented_exports(self) -> None:
        """Test that the top-level package keeps stable facade symbols."""
        assert etlplus.__all__ == ['__author__', '__version__', 'Config']
        assert etlplus.Config is Config
        assert isinstance(etlplus.__version__, str)
        assert etlplus.__version__
