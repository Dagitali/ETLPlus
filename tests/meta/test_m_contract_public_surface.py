"""
:mod:`tests.meta.test_m_contract_public_surface` module.

Contract tests for ETLPlus stable CLI and import surfaces.
"""

from __future__ import annotations

from types import ModuleType

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
    'schedule',
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

type ExportCase = tuple[ModuleType, str, object]

API_EXPORTS: tuple[ExportCase, ...] = (
    (api_pkg, 'EndpointClient', EndpointClient),
    (api_pkg, 'PaginationConfig', PaginationConfig),
    (api_pkg, 'PaginationInput', PaginationInput),
    (api_pkg, 'PaginationType', PaginationType),
    (api_pkg, 'Paginator', Paginator),
    (api_pkg, 'RateLimitConfig', RateLimitConfig),
    (api_pkg, 'RateLimitOverrides', RateLimitOverrides),
    (api_pkg, 'RateLimiter', RateLimiter),
    (api_pkg, 'RetryManager', RetryManager),
)
FILE_EXPORTS: tuple[ExportCase, ...] = (
    (file_pkg, 'BoundFileHandler', BoundFileHandler),
    (file_pkg, 'File', File),
    (file_pkg, 'FileFormat', FileFormat),
    (file_pkg, 'ReadOptions', ReadOptions),
    (file_pkg, 'WriteOptions', WriteOptions),
)
OPS_EXPORTS: tuple[ExportCase, ...] = (
    (ops_pkg, 'extract', extract),
    (ops_pkg, 'load', load),
    (ops_pkg, 'run', run),
    (ops_pkg, 'run_pipeline', run_pipeline),
    (ops_pkg, 'transform', transform),
    (ops_pkg, 'validate', validate),
    (ops_pkg, 'validate_schema', validate_schema),
    (ops_pkg, 'FieldRulesDict', FieldRulesDict),
    (ops_pkg, 'FieldValidationDict', FieldValidationDict),
    (ops_pkg, 'ValidationDict', ValidationDict),
)
OPS_TRANSFORMATION_EXPORTS: tuple[ExportCase, ...] = (
    (aggregate_tx_mod, 'apply_aggregate', apply_aggregate),
    (aggregate_tx_mod, 'apply_aggregate_step', apply_aggregate_step),
    (filter_tx_mod, 'apply_filter', apply_filter),
    (filter_tx_mod, 'apply_filter_step', apply_filter_step),
    (map_tx_mod, 'apply_map', apply_map),
    (map_tx_mod, 'apply_map_step', apply_map_step),
    (select_tx_mod, 'apply_select', apply_select),
    (select_tx_mod, 'apply_select_step', apply_select_step),
    (select_tx_mod, 'is_plain_fields_list', is_plain_fields_list),
    (select_tx_mod, 'is_sequence_not_text', is_sequence_not_text),
    (sort_tx_mod, 'apply_sort', apply_sort),
    (sort_tx_mod, 'apply_sort_step', apply_sort_step),
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _export_case_id(value: object) -> str:
    """Return stable pytest IDs for export contract cases."""
    return getattr(value, '__name__', str(value))


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

    @pytest.mark.parametrize(
        ('module', 'name', 'expected'),
        API_EXPORTS,
        ids=_export_case_id,
    )
    def test_api_package_keeps_documented_core_exports(
        self,
        module: ModuleType,
        name: str,
        expected: object,
    ) -> None:
        """Test that :mod:`etlplus.api` keeps core documented imports."""
        assert name in module.__all__
        assert getattr(module, name) is expected

    @pytest.mark.parametrize(
        ('module', 'name', 'expected'),
        FILE_EXPORTS,
        ids=_export_case_id,
    )
    def test_file_package_keeps_handler_authoring_facade(
        self,
        module: ModuleType,
        name: str,
        expected: object,
    ) -> None:
        """Test that :mod:`etlplus.file` exposes the handler authoring layer."""
        assert name in module.__all__
        assert getattr(module, name) is expected

    def test_history_package_keeps_documented_runtime_metadata(self) -> None:
        """Test that :mod:`etlplus.history` keeps schema metadata exports."""
        assert 'HISTORY_SCHEMA_VERSION' in history_pkg.__all__
        assert history_pkg.HISTORY_SCHEMA_VERSION == HISTORY_SCHEMA_VERSION

    def test_ops_package_keeps_documented_export_order(self) -> None:
        """Test that :mod:`etlplus.ops` keeps documented export ordering."""
        assert ops_pkg.__all__ == EXPECTED_OPS_EXPORTS

    @pytest.mark.parametrize(
        ('module', 'name', 'expected'),
        OPS_EXPORTS,
        ids=_export_case_id,
    )
    def test_ops_package_keeps_documented_exports(
        self,
        module: ModuleType,
        name: str,
        expected: object,
    ) -> None:
        """Test that :mod:`etlplus.ops` keeps documented re-exports."""
        assert name in module.__all__
        assert getattr(module, name) is expected

    @pytest.mark.parametrize(
        ('module', 'name', 'expected'),
        OPS_TRANSFORMATION_EXPORTS,
        ids=_export_case_id,
    )
    def test_ops_transformations_modules_keep_documented_helpers(
        self,
        module: ModuleType,
        name: str,
        expected: object,
    ) -> None:
        """Test that step-level transform modules keep documented helpers."""
        assert name in module.__all__
        assert getattr(module, name) is expected

    def test_top_level_package_keeps_documented_exports(self) -> None:
        """Test that the top-level package keeps stable facade symbols."""
        assert etlplus.__all__ == ['__author__', '__version__', 'Config']
        assert etlplus.Config is Config
        assert isinstance(etlplus.__version__, str)
        assert etlplus.__version__
