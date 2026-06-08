"""
:mod:`tests.meta.test_m_contract_public_surface` module.

Contract tests for ETLPlus stable CLI and import surfaces.
"""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest

import etlplus
import etlplus.api as api_pkg
import etlplus.api._retry_manager as retry_manager_mod
import etlplus.cli as cli_pkg
import etlplus.cli._commands as commands_mod
import etlplus.file as file_pkg
import etlplus.file._core as file_core_mod
import etlplus.file._enums as file_enums_mod
import etlplus.file.base as file_base_mod
import etlplus.history as history_pkg
import etlplus.ops as ops_pkg
import etlplus.ops._enums as ops_enums_mod
import etlplus.ops._validation as ops_validation_mod
import etlplus.ops.transformations.aggregate as aggregate_tx_mod
import etlplus.ops.transformations.filter as filter_tx_mod
import etlplus.ops.transformations.map as map_tx_mod
import etlplus.ops.transformations.select as select_tx_mod
import etlplus.ops.transformations.sort as sort_tx_mod
from etlplus import Config
from etlplus.api import endpoint_client as endpoint_client_mod
from etlplus.api import pagination as pagination_mod
from etlplus.api import rate_limiting as rate_limiting_mod
from etlplus.cli import main as main_mod
from etlplus.history import HISTORY_SCHEMA_VERSION

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKERS ========================================================== #


# Directory-level marker for meta tests.
pytestmark = [pytest.mark.meta, pytest.mark.contract]


# SECTION: IMPORTS ========================================================== #


extract_mod = importlib.import_module('etlplus.ops.extract')
load_mod = importlib.import_module('etlplus.ops.load')
run_mod = importlib.import_module('etlplus.ops.run')
transform_mod = importlib.import_module('etlplus.ops.transform')
validate_mod = importlib.import_module('etlplus.ops.validate')


# SECTION: TYPE ALIASES ===================================================== #


type ExportCase = tuple[ModuleType, str, ModuleType]


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
    'ui',
    'validate',
}

EXPECTED_CLI_EXPORTS = [
    'main',
]
EXPECTED_HISTORY_EXPORTS = [
    'HistoryStore',
    'JsonlHistoryStore',
    'RunCompletion',
    'RunRecord',
    'RunState',
    'SQLiteHistoryStore',
    'HISTORY_SCHEMA_VERSION',
]
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
EXPECTED_TOP_LEVEL_EXPORTS = [
    '__author__',
    '__version__',
    'Config',
]


API_EXPORTS: tuple[ExportCase, ...] = (
    (api_pkg, 'EndpointClient', endpoint_client_mod),
    (api_pkg, 'PaginationConfig', pagination_mod),
    (api_pkg, 'PaginationInput', pagination_mod),
    (api_pkg, 'PaginationType', pagination_mod),
    (api_pkg, 'Paginator', pagination_mod),
    (api_pkg, 'RateLimitConfig', rate_limiting_mod),
    (api_pkg, 'RateLimitOverrides', rate_limiting_mod),
    (api_pkg, 'RateLimiter', rate_limiting_mod),
    (api_pkg, 'RetryManager', retry_manager_mod),
)
FILE_EXPORTS: tuple[ExportCase, ...] = (
    (file_pkg, 'BoundFileHandler', file_base_mod),
    (file_pkg, 'File', file_core_mod),
    (file_pkg, 'FileFormat', file_enums_mod),
    (file_pkg, 'ReadOptions', file_base_mod),
    (file_pkg, 'WriteOptions', file_base_mod),
)
OPS_EXPORTS: tuple[ExportCase, ...] = (
    (ops_pkg, 'ValidationSettings', ops_validation_mod),
    (ops_pkg, 'AggregateName', ops_enums_mod),
    (ops_pkg, 'OperatorName', ops_enums_mod),
    (ops_pkg, 'PipelineStep', ops_enums_mod),
    (ops_pkg, 'extract', extract_mod),
    (ops_pkg, 'load', load_mod),
    (ops_pkg, 'maybe_validate', ops_validation_mod),
    (ops_pkg, 'run', run_mod),
    (ops_pkg, 'run_pipeline', run_mod),
    (ops_pkg, 'transform', transform_mod),
    (ops_pkg, 'validate', validate_mod),
    (ops_pkg, 'validate_schema', validate_mod),
    (ops_pkg, 'FieldRulesDict', validate_mod),
    (ops_pkg, 'FieldValidationDict', validate_mod),
    (ops_pkg, 'ValidationDict', validate_mod),
    (ops_pkg, 'ValidationResultDict', ops_validation_mod),
)
OPS_TRANSFORMATION_EXPORTS: tuple[ExportCase, ...] = (
    (aggregate_tx_mod, 'apply_aggregate', aggregate_tx_mod),
    (aggregate_tx_mod, 'apply_aggregate_step', aggregate_tx_mod),
    (filter_tx_mod, 'apply_filter', filter_tx_mod),
    (filter_tx_mod, 'apply_filter_step', filter_tx_mod),
    (map_tx_mod, 'apply_map', map_tx_mod),
    (map_tx_mod, 'apply_map_step', map_tx_mod),
    (select_tx_mod, 'apply_select', select_tx_mod),
    (select_tx_mod, 'apply_select_step', select_tx_mod),
    (select_tx_mod, 'is_plain_fields_list', select_tx_mod),
    (select_tx_mod, 'is_sequence_not_text', select_tx_mod),
    (sort_tx_mod, 'apply_sort', sort_tx_mod),
    (sort_tx_mod, 'apply_sort_step', sort_tx_mod),
)

DOCUMENTED_EXPORTS = tuple(
    pytest.param(module, name, source_module, id=f'{module.__name__}.{name}')
    for module, name, source_module in (
        *API_EXPORTS,
        *FILE_EXPORTS,
        *OPS_EXPORTS,
        *OPS_TRANSFORMATION_EXPORTS,
    )
)
PACKAGE_EXPORT_ORDER_CASES = (
    pytest.param(etlplus, EXPECTED_TOP_LEVEL_EXPORTS, id='etlplus'),
    pytest.param(cli_pkg, EXPECTED_CLI_EXPORTS, id='etlplus.cli'),
    pytest.param(history_pkg, EXPECTED_HISTORY_EXPORTS, id='etlplus.history'),
    pytest.param(ops_pkg, EXPECTED_OPS_EXPORTS, id='etlplus.ops'),
)


# SECTION: TESTS ============================================================ #


class TestStableCliSurface:
    """Contract tests for the documented stable CLI command surface."""

    def test_cli_package_export_points_to_main_entrypoint(self) -> None:
        """Test that the public CLI package export remains stable."""
        assert cli_pkg.main is main_mod

    def test_history_view_shim_is_not_importable(self) -> None:
        """Test that history view ownership stays under :mod:`etlplus.history`."""
        assert importlib.util.find_spec('etlplus.cli._handlers._history_view') is None

    def test_typer_app_exposes_documented_root_commands(self) -> None:
        """Test that the Typer app keeps the documented command set."""
        command_names = {
            command.name
            for command in commands_mod.app.registered_commands
            if command.name is not None
        }
        assert command_names == EXPECTED_CLI_COMMANDS


class TestStableImportSurface:
    """Contract tests for the documented stable Python import surface."""

    def test_history_package_keeps_documented_runtime_metadata(self) -> None:
        """Test that :mod:`etlplus.history` keeps schema metadata exports."""
        assert history_pkg.HISTORY_SCHEMA_VERSION == HISTORY_SCHEMA_VERSION

    @pytest.mark.parametrize(
        ('module', 'expected_exports'),
        PACKAGE_EXPORT_ORDER_CASES,
    )
    def test_packages_keep_documented_export_order(
        self,
        module: ModuleType,
        expected_exports: list[str],
    ) -> None:
        """Test that public packages keep documented export ordering."""
        assert module.__all__ == expected_exports

    @pytest.mark.parametrize(
        ('module', 'name', 'source_module'),
        DOCUMENTED_EXPORTS,
    )
    def test_packages_keep_documented_exports(
        self,
        module: ModuleType,
        name: str,
        source_module: ModuleType,
    ) -> None:
        """Test that public packages keep documented exports."""
        assert name in module.__all__
        assert getattr(module, name) is getattr(source_module, name)

    def test_top_level_package_keeps_lazy_config_and_version_exports(self) -> None:
        """Test that the top-level package keeps stable lazy facade symbols."""
        assert etlplus.Config is Config
        assert isinstance(etlplus.__version__, str)
        assert etlplus.__version__
