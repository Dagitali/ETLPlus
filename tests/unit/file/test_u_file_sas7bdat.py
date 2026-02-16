"""
:mod:`tests.unit.file.test_u_file_sas7bdat` module.

Unit tests for :mod:`etlplus.file.sas7bdat`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sas7bdat as mod
from etlplus.file.base import ReadOptions

from .pytest_file_contract_mixins import PathMixin
from .pytest_file_contract_utils import patch_dependency_resolver_unreachable
from .pytest_file_contract_utils import patch_dependency_resolver_value
from .pytest_file_contracts import ReadOnlyScientificDatasetModuleContract
from .pytest_file_support import DictRecordsFrameStub
from .pytest_file_support import PandasReadSasStub
from .pytest_file_types import OptionalModuleInstaller

# SECTION: TESTS ============================================================ #


class TestSas7bdatReadOnly(ReadOnlyScientificDatasetModuleContract):
    """Read-only scientific contract tests for :mod:`etlplus.file.sas7bdat`."""

    # pylint: disable=unused-variable

    module = mod
    handler_cls = mod.Sas7bdatFile
    format_name = 'sas7bdat'
    unknown_dataset_error_pattern = 'supports only dataset key'

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Ensure dataset-key validation occurs before optional imports."""
        _ = tmp_path
        patch_dependency_resolver_unreachable(monkeypatch, mod)
        patch_dependency_resolver_unreachable(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
        )


class TestSas7bdatRead(PathMixin):
    """Unit tests for :func:`etlplus.file.sas7bdat.read`."""

    format_name = 'sas7bdat'

    def test_list_datasets_returns_default_key(self) -> None:
        """Test list_datasets exposing the single supported key."""
        assert mod.Sas7bdatFile().list_datasets(Path('ignored.sas7bdat')) == [
            'data',
        ]

    def test_read_dataset_accepts_default_key_via_options(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test option-based default dataset selection for read_dataset."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame)
        self._install_dependency_stubs(monkeypatch, pandas)
        path = self.format_path(tmp_path)

        result = mod.Sas7bdatFile().read_dataset(
            path,
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]

    def test_read_falls_back_when_format_kwarg_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read fallback when pandas rejects the format keyword."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame, fail_on_format_kwarg=True)
        self._install_dependency_stubs(monkeypatch, pandas)
        path = self.format_path(tmp_path)

        result = mod.Sas7bdatFile().read(path)

        assert result == [{'id': 1}]
        pandas.assert_fallback_read_calls(path, format_name='sas7bdat')

    def test_read_uses_format_hint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that read requests the SAS7BDAT format hint when supported."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame)
        self._install_dependency_stubs(monkeypatch, pandas)
        path = self.format_path(tmp_path)

        result = mod.Sas7bdatFile().read(path)

        assert result == [{'id': 1}]
        pandas.assert_single_read_call(path, format_name='sas7bdat')

    @staticmethod
    def _install_dependency_stubs(
        monkeypatch: pytest.MonkeyPatch,
        pandas: PandasReadSasStub,
    ) -> None:
        """Install deterministic dependency stubs for SAS7BDAT tests."""
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            value=object(),
        )
        patch_dependency_resolver_value(
            monkeypatch,
            mod,
            resolver_name='get_pandas',
            value=pandas,
        )
