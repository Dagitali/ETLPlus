"""
:mod:`etlplus.file._module_callables` module.

Shared helpers for optional module method resolution and calls.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
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


# SECTION: INTERNAL CLASSES ================================================ #


@dataclass(slots=True)
class _ModuleCallContext:
    """
    Immutable context for module-method operations.

    Attributes
    ----------
    module : Any | None
        Optional module object to resolve methods on.
    format_name : str
        Human-readable format name for templated messages.
    operation : ModuleOperation
        Operation name for templated messages (e.g. "read" or "write").
    module_name : str
        Human-readable module name for templated messages.
    """

    # -- Instance Attributes -- #

    module: Any | None
    format_name: str
    operation: ModuleOperation
    module_name: str = 'module'

    # -- Instance Methods -- #

    def call(
        self,
        *,
        method_name: str,
        args: tuple[Any, ...] = (),
        kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Resolve and call one required module method.
        """
        module_method = resolve_module_method(
            self.require_module(),
            method_name,
        )
        if module_method is None:
            self.raise_missing_module_method(method_name)
        return module_method(*args, **_call_kwargs(kwargs))

    def raise_missing_module_method(
        self,
        method_name: str,
    ) -> NoReturn:
        """
        Raise the standardized missing-method error for this context.

        Parameters
        ----------
        method_name : str
            Method name that is missing on the module.
        """
        raise ImportError(
            f'{self.format_name} {self.operation} support requires '
            f'"{self.module_name}" with {method_name}().',
        )

    def require_module(self) -> Any:
        """
        Return the required module object or raise a runtime dependency error.
        """
        if self.module is None:  # pragma: no cover - guarded by mixin flags
            raise RuntimeError(
                f'{self.module_name} dependency is required for '
                f'{self.format_name} {self.operation}',
            )
        return self.module


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

    Parameters
    ----------
    module : Any | None
        Optional module object to resolve the method on.
    format_name : str
        Human-readable format name for templated messages.
    method_name : str
        Method name to resolve and call on the module.
    operation : ModuleOperation
        Operation name for templated messages (e.g. "read" or "write").
    module_name : str, optional
        Human-readable module name for templated messages. Default is
        `'module'`.
    args : tuple[Any, ...], optional
        Optional tuple of positional arguments to pass to the method. Default
        is ``()``.
    kwargs : Mapping[str, Any] | None, optional
        Optional mapping of keyword arguments to pass to the method. Default
        is ``None``.

    Returns
    -------
    Any
        The result of the module method call.
    """
    return _ModuleCallContext(
        module=module,
        format_name=format_name,
        operation=operation,
        module_name=module_name,
    ).call(
        method_name=method_name,
        args=args,
        kwargs=kwargs,
    )


def raise_missing_module_method(
    *,
    format_name: str,
    module_name: str = 'module',
    method_name: str,
    operation: ModuleOperation,
) -> NoReturn:
    """
    Raise a consistent import error for missing module methods.

    Parameters
    ----------
    format_name : str
        Human-readable format name for templated messages.
    module_name : str, optional
        Human-readable module name for templated messages. Default is
        `'module'`.
    method_name : str
        Method name that is missing on the module.
    operation : ModuleOperation
        Operation name for templated messages (e.g. "read" or "write").

    Raises
    ------
    ImportError
        Always raised with a consistent message about the missing method.
    """
    _ModuleCallContext(
        module=object(),
        format_name=format_name,
        operation=operation,
        module_name=module_name,
    ).raise_missing_module_method(method_name)


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
    return _ModuleCallContext(
        module=module,
        format_name=format_name,
        operation=operation,
        module_name=module_name,
    ).require_module()


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
