"""
:mod:`tests.unit.file.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.file`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup across file-focused unit
    tests.
"""

from __future__ import annotations

import math
import numbers
from collections.abc import Callable
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from typing import Any
from typing import Literal
from typing import cast

import pytest

import etlplus.file._imports as import_helpers
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.stub import StubFileHandlerABC
from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_numeric_value(
    value: object,
) -> object:
    """Coerce numeric scalars into stable Python numeric types."""
    if isinstance(value, numbers.Real):
        try:
            numeric = float(value)
            if math.isnan(numeric):
                return None
        except (TypeError, ValueError):
            return value
        if numeric.is_integer():
            return int(numeric)
        return float(numeric)
    return value


# SECTION: FUNCTIONS ======================================================== #


def assert_single_dataset_rejects_non_default_key(
    handler: SingleDatasetScientificFileHandlerABC,
    *,
    suffix: str,
) -> None:
    """Assert single-dataset scientific handlers reject non-default keys."""
    bad_dataset = 'not_default_dataset'
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.read_dataset(
            Path(f'ignored.{suffix}'),
            dataset=bad_dataset,
        )
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.write_dataset(
            Path(f'ignored.{suffix}'),
            [],
            dataset=bad_dataset,
        )


def assert_stub_module_contract(
    module: ModuleType,
    handler_cls: type[StubFileHandlerABC],
    *,
    format_name: str,
    tmp_path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert baseline contract for a placeholder stub module."""
    if write_payload is None:
        write_payload = [{'id': 1}]

    assert issubclass(handler_cls, StubFileHandlerABC)
    assert handler_cls.format.value == format_name

    path = tmp_path / f'data.{format_name}'
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} read is not implemented yet',
    ):
        module.read(path)
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} write is not implemented yet',
    ):
        module.write(path, write_payload)


def assert_stub_module_operation_raises(
    module: ModuleType,
    *,
    format_name: str,
    operation: Literal['read', 'write'],
    path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert one stub module operation raises :class:`NotImplementedError`."""
    if write_payload is None:
        write_payload = [{'id': 1}]

    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} {operation} is not implemented yet',
    ):
        if operation == 'read':
            module.read(path)
        else:
            module.write(path, write_payload)


def make_import_error_reader_module(
    method_name: str,
) -> object:
    """
    Build a module-like object whose reader method raises ImportError.

    Parameters
    ----------
    method_name : str
        Reader method name to define (for example, ``"read_excel"``).

    Returns
    -------
    object
        Module-like object with one failing reader method.
    """

    class _FailModule:
        """Module stub exposing one reader that always raises ImportError."""

        def __getattribute__(self, name: str) -> Any:
            if name != method_name:
                return super().__getattribute__(name)

            def _fail_reader(
                *args: object,
                **kwargs: object,
            ) -> object:  # noqa: ARG001
                raise ImportError('missing')

            return _fail_reader

    return _FailModule()


def make_import_error_writer_module() -> object:
    """
    Build a pandas-like module whose DataFrame writes raise ImportError.

    Returns
    -------
    object
        Module-like object with ``DataFrame.from_records`` that returns a
        failing frame.
    """

    class _FailFrame:
        """Frame stub whose write-like attributes raise ImportError."""

        def __getattr__(self, name: str) -> object:
            if not name.startswith('to_'):
                raise AttributeError(name)

            def _fail_writer(
                *args: object,
                **kwargs: object,
            ) -> None:  # noqa: ARG001
                raise ImportError('missing')

            return _fail_writer

    class _FailModule:
        """Module stub exposing failing ``DataFrame.from_records``."""

        # pylint: disable=unused-argument

        class DataFrame:  # noqa: D106
            """Minimal DataFrame namespace for write-path tests."""

            @staticmethod
            def from_records(
                records: list[dict[str, object]],
            ) -> _FailFrame:  # noqa: ARG002
                return _FailFrame()

    return _FailModule()


def normalize_numeric_records(
    records: JSONData,
) -> JSONData:
    """
    Normalize numeric record values for deterministic comparisons.

    Parameters
    ----------
    records : JSONData
        Record payloads to normalize.

    Returns
    -------
    JSONData
        Normalized record payloads.
    """
    if isinstance(records, list):
        normalized: list[JSONDict] = []
        for row in records:
            if not isinstance(row, dict):
                normalized.append(row)
                continue
            cleaned: JSONDict = {}
            for key, value in row.items():
                cleaned[key] = _coerce_numeric_value(value)
            normalized.append(cleaned)
        return normalized
    return records


def normalize_xml_payload(payload: JSONData) -> JSONData:
    """
    Normalize XML payloads to list-based item structures when possible.

    Parameters
    ----------
    payload : JSONData
        XML payload to normalize.

    Returns
    -------
    JSONData
        Normalized XML payload.
    """
    if not isinstance(payload, dict):
        return payload
    root = payload.get('root')
    if not isinstance(root, dict):
        return payload
    items = root.get('items')
    if isinstance(items, dict):
        root = {**root, 'items': [items]}
        return {**payload, 'root': root}
    return payload


def require_optional_modules(
    *modules: str,
) -> None:
    """
    Skip the test when optional dependencies are missing.

    Parameters
    ----------
    *modules : str
        Module names to verify via ``pytest.importorskip``.
    """
    for module in modules:
        pytest.importorskip(module)


# SECTION: CLASSES ========================================================== #


class StubModuleContract:
    """
    Reusable contract suite for placeholder/stub format modules.

    Subclasses must provide:
    - ``module``
    - ``handler_cls``
    - ``format_name``
    """

    module: ModuleType
    handler_cls: type[StubFileHandlerABC]
    format_name: str

    def test_handler_inherits_stub_abc(self) -> None:
        """Test handler metadata and inheritance contract."""
        assert issubclass(self.handler_cls, StubFileHandlerABC)
        assert self.handler_cls.format.value == self.format_name

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            self.module,
            format_name=self.format_name,
            operation=cast(Literal['read', 'write'], operation),
            path=tmp_path / f'data.{self.format_name}',
        )


class SingleDatasetHandlerContract:
    """
    Reusable contract suite for single-dataset scientific handlers.

    Subclasses must provide:
    - ``module``
    - ``handler_cls``
    - ``format_name``
    """

    module: ModuleType
    handler_cls: type[SingleDatasetScientificFileHandlerABC]
    format_name: str

    @pytest.fixture
    def handler(self) -> SingleDatasetScientificFileHandlerABC:
        """Create a handler instance for contract tests."""
        return self.handler_cls()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test single-dataset scientific class contract."""
        assert issubclass(
            self.handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert self.handler_cls.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: SingleDatasetScientificFileHandlerABC,
    ) -> None:
        """Test non-default dataset keys are rejected."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix=self.format_name,
        )


class SingleDatasetWritableContract(SingleDatasetHandlerContract):
    """
    Reusable suite for writable single-dataset scientific handlers.
    """

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty write payloads return zero and create no file."""
        path = tmp_path / f'data.{self.format_name}'
        assert self.module.write(path, []) == 0
        assert not path.exists()


class SingleDatasetPlaceholderContract(SingleDatasetHandlerContract):
    """
    Reusable suite for placeholder single-dataset scientific handlers.
    """

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test placeholder read/write behavior for module-level wrappers."""
        path = tmp_path / f'data.{self.format_name}'
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            if operation == 'read':
                self.module.read(path)
            else:
                self.module.write(path, [{'id': 1}])


class PandasModuleStub:
    """
    Minimal pandas-module stub with read and DataFrame factory helpers.

    Parameters
    ----------
    frame : RecordsFrameStub
        Frame object returned by read operations.
    """

    # pylint: disable=invalid-name

    def __init__(
        self,
        frame: RecordsFrameStub,
    ) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: RecordsFrameStub | None = None

        def _from_records(
            records: list[dict[str, object]],
        ) -> RecordsFrameStub:
            created = RecordsFrameStub(records)
            self.last_frame = created
            return created

        self.DataFrame = type(
            'DataFrame',
            (),
            {'from_records': staticmethod(_from_records)},
        )

    def _record_read(
        self,
        path: Path,
        **kwargs: object,
    ) -> RecordsFrameStub:
        call: dict[str, object] = {'path': path, **kwargs}
        self.read_calls.append(call)
        return self._frame

    def read_excel(
        self,
        path: Path,
        *,
        engine: str | None = None,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_excel``.

        Parameters
        ----------
        path : Path
            Input path.
        engine : str | None, optional
            Optional engine argument.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        if engine is None:
            return self._record_read(path)
        return self._record_read(path, engine=engine)

    def read_parquet(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_parquet``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)

    def read_feather(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_feather``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)

    def read_orc(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_orc``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)


class RecordsFrameStub:
    """
    Minimal frame stub that mimics pandas record/table APIs.

    Parameters
    ----------
    records : list[dict[str, object]]
        In-memory records returned by :meth:`to_dict`.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = list(records)
        self.to_excel_calls: list[dict[str, object]] = []
        self.to_parquet_calls: list[dict[str, object]] = []
        self.to_feather_calls: list[dict[str, object]] = []
        self.to_orc_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """
        Return record payloads in ``records`` orientation.

        Parameters
        ----------
        orient : str
            Requested output orientation.

        Returns
        -------
        list[dict[str, object]]
            Record payloads.
        """
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
        engine: str | None = None,
    ) -> None:
        """
        Record an Excel write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        engine : str | None, optional
            Optional pandas engine argument.
        """
        call: dict[str, object] = {'path': path, 'index': index}
        if engine is not None:
            call['engine'] = engine
        self.to_excel_calls.append(call)

    def to_feather(
        self,
        path: Path,
    ) -> None:
        """
        Record a feather write call.

        Parameters
        ----------
        path : Path
            Target output path.
        """
        self.to_feather_calls.append({'path': path})

    def to_orc(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """
        Record an ORC write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        """
        self.to_orc_calls.append({'path': path, 'index': index})

    def to_parquet(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """
        Record a parquet write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        """
        self.to_parquet_calls.append({'path': path, 'index': index})


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='make_import_error_reader')
def make_import_error_reader_fixture() -> Callable[[str], object]:
    """
    Build module-like objects with one failing reader method.

    Returns
    -------
    Callable[[str], object]
        Factory that maps a method name to a failing module-like object.
    """
    return make_import_error_reader_module


@pytest.fixture(name='make_import_error_writer')
def make_import_error_writer_fixture() -> Callable[[], object]:
    """
    Build pandas-like module objects with failing write paths.

    Returns
    -------
    Callable[[], object]
        Factory returning a failing module-like object.
    """
    return make_import_error_writer_module


@pytest.fixture(name='make_pandas_stub')
def make_pandas_stub_fixture() -> Callable[
    [RecordsFrameStub],
    PandasModuleStub,
]:
    """
    Build :class:`PandasModuleStub` instances for tests.

    Returns
    -------
    Callable[[RecordsFrameStub], PandasModuleStub]
        pandas module stub factory.
    """
    return PandasModuleStub


@pytest.fixture(name='make_records_frame')
def make_records_frame_fixture() -> Callable[
    [list[dict[str, object]]],
    RecordsFrameStub,
]:
    """
    Build :class:`RecordsFrameStub` instances for tests.

    Returns
    -------
    Callable[[list[dict[str, object]]], RecordsFrameStub]
        Frame factory.
    """
    return RecordsFrameStub


@pytest.fixture(name='optional_module_stub')
def optional_module_stub_fixture() -> Generator[
    Callable[[dict[str, object]], None]
]:
    """
    Install stub modules into the optional import cache.

    Clears the cache for deterministic tests, and restores it afterward.
    """
    cache = import_helpers._MODULE_CACHE  # pylint: disable=protected-access
    original = dict(cache)
    cache.clear()

    def _install(mapping: dict[str, object]) -> None:
        cache.update(mapping)

    try:
        yield _install
    finally:
        cache.clear()
        cache.update(original)
