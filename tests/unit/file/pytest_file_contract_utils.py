"""
:mod:`tests.unit.file.pytest_file_contract_utils` module.

Shared helper functions for unit file contract tests.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any
from typing import Literal
from typing import cast

import pytest

from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.types import JSONData

from ...pytest_file_common import resolve_module_handler

# SECTION: TYPE ALIASES ===================================================== #


type Operation = Literal['read', 'write']


# SECTION: FUNCTIONS ======================================================== #


def module_handler(
    module: ModuleType,
) -> Any:
    """Return the singleton handler instance defined by a file module."""
    return resolve_module_handler(module)


def call_module_operation(
    module: ModuleType,
    *,
    operation: Operation,
    path: Path,
    write_payload: JSONData | None = None,
) -> JSONData | int:
    """Invoke handler ``read``/``write`` without deprecated module wrappers."""
    handler = module_handler(module)
    if operation == 'read':
        return cast(JSONData, handler.read(path))
    payload = make_payload('list') if write_payload is None else write_payload
    return cast(int, handler.write(path, payload))


def make_payload(
    kind: Literal['dict', 'list', 'read'],
    **kwargs: object,
) -> JSONData:
    """Build common JSON payload shapes used across test contracts."""
    if (payload := kwargs.get('payload')) is not None:
        return cast(JSONData, payload)

    match kind:
        case 'dict':
            key = cast(str, kwargs.get('key', 'id'))
            return cast(JSONData, {key: kwargs.get('value', 1)})
        case 'list':
            if (records := kwargs.get('records')) is not None:
                return cast(JSONData, records)
            if (record := kwargs.get('record')) is not None:
                return cast(JSONData, [record])
            return cast(JSONData, [make_payload('dict')])
        case _:
            if (result := kwargs.get('result')) is not None:
                return cast(JSONData, result)
            return cast(JSONData, {'ok': bool(kwargs.get('ok', True))})


def assert_single_dataset_rejects_non_default_key(
    handler: SingleDatasetScientificFileHandlerABC,
    *,
    suffix: str,
) -> None:
    """Assert single-dataset scientific handlers reject non-default keys."""
    bad_dataset = 'not_default_dataset'
    path = Path(f'ignored.{suffix}')
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.read_dataset(path, dataset=bad_dataset)
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.write_dataset(path, [], dataset=bad_dataset)


def assert_stub_module_operation_raises(
    module: ModuleType,
    *,
    format_name: str,
    operation: Operation,
    path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert one stub module operation raises :class:`NotImplementedError`."""
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} {operation} is not implemented yet',
    ):
        call_module_operation(
            module,
            operation=operation,
            path=path,
            write_payload=write_payload,
        )


def patch_dependency_resolver_unreachable(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    *,
    resolver_name: str = 'get_dependency',
) -> None:
    """Patch one dependency resolver to raise if a test triggers it."""
    monkeypatch.setattr(
        module,
        resolver_name,
        _raise_unexpected_dependency_call,
    )


def patch_dependency_resolver_value(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    *,
    resolver_name: str = 'get_dependency',
    value: object,
) -> None:
    """Patch one dependency resolver to return a deterministic value."""
    # pylint: disable=unused-argument

    def _return_value(
        *args: object,
        **kwargs: object,
    ) -> object:  # noqa: ARG001
        return value

    monkeypatch.setattr(module, resolver_name, _return_value)


def _raise_unexpected_dependency_call(
    *args: object,
    **kwargs: object,
) -> object:  # noqa: ARG001
    """Raise when a dependency resolver is called unexpectedly in tests."""
    raise AssertionError('dependency resolver should not be called')
