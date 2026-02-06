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

import pytest

import etlplus.file._imports as import_helpers
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
