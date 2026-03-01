"""
:mod:`etlplus.file._module_callables` module.

Shared helpers for optional module method resolution and calls.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Literal
from typing import NoReturn

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'call_module_method',
    'raise_missing_module_method',
    'read_module_frame',
    'read_module_frame_if_supported',
    'require_module',
    'resolve_module_method',
    'write_module_frame',
]


# SECTION: TYPE ALIASES ===================================================== #


type ModuleOperation = Literal['read', 'write']


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _call_kwargs(
    kwargs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """
    Return a mutable kwargs dictionary for dynamic module calls.

    Parameters
    ----------
    kwargs : Mapping[str, Any] | None
        Optional kwargs mapping to copy for module method calls.

    Returns
    -------
    dict[str, Any]
        A mutable kwargs dictionary for module method calls.
    """
    return {} if kwargs is None else dict(kwargs)


# SECTION: FUNCTIONS ======================================================== #


def call_module_method(
    module: Any | None,
    *,
    format_name: str,
    method_name: str,
    operation: ModuleOperation,
    module_name: str = 'module',
    args: tuple[Any, ...] = (),
    kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """
    Resolve and call one required module method with standardized errors.
    """
    module_method = resolve_module_method(
        require_module(
            module,
            format_name=format_name,
            operation=operation,
            module_name=module_name,
        ),
        method_name,
    )
    if module_method is None:
        raise_missing_module_method(
            format_name=format_name,
            module_name=module_name,
            method_name=method_name,
            operation=operation,
        )
    return module_method(*args, **_call_kwargs(kwargs))


def raise_missing_module_method(
    *,
    format_name: str,
    module_name: str = 'module',
    method_name: str,
    operation: ModuleOperation,
) -> NoReturn:
    """
    Raise a consistent import error for missing module methods.
    """
    raise ImportError(
        f'{format_name} {operation} support requires "{module_name}" '
        f'with {method_name}().',
    )


def require_module(
    module: Any | None,
    *,
    format_name: str,
    operation: ModuleOperation,
    module_name: str = 'module',
) -> Any:
    """
    Return a required module object or raise a runtime dependency error.
    """
    if module is None:  # pragma: no cover - guarded by mixin flags
        raise RuntimeError(
            f'{module_name} dependency is required for {format_name} '
            f'{operation}',
        )
    return module


def resolve_module_method(
    module: Any | None,
    method_name: str,
) -> Callable[..., Any] | None:
    """
    Return one callable by name when available on the provided module.
    """
    if module is None:
        return None
    method = getattr(module, method_name, None)
    return method if callable(method) else None


def read_module_frame(
    module: Any | None,
    *,
    format_name: str,
    module_name: str = 'module',
    method_name: str,
    path: Path,
) -> Any:
    """
    Read one table-like frame via one named module reader method.
    """
    frame, _meta = call_module_method(
        module,
        format_name=format_name,
        method_name=method_name,
        operation='read',
        module_name=module_name,
        args=(str(path),),
    )
    return frame


def read_module_frame_if_supported(
    module: Any | None,
    *,
    method_name: str,
    path: Path,
) -> Any | None:
    """
    Read one frame when the named module reader method exists.
    """
    reader = resolve_module_method(module, method_name)
    if reader is None:
        return None
    frame, _meta = reader(str(path))
    return frame


def write_module_frame(
    module: Any | None,
    *,
    format_name: str,
    module_name: str = 'module',
    method_name: str,
    frame: Any,
    path: Path,
) -> None:
    """
    Write one table-like frame via one named module writer method.
    """
    call_module_method(
        module,
        format_name=format_name,
        method_name=method_name,
        operation='write',
        module_name=module_name,
        args=(frame, str(path)),
    )
