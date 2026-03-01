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
    'read_module_frame',
    'read_module_frame_if_supported',
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

    # -- Static Methods -- #

    @staticmethod
    def build_missing_module_method_error(
        *,
        format_name: str,
        module_name: str,
        method_name: str,
        operation: ModuleOperation,
    ) -> ImportError:
        """
        Build a standardized missing-method ImportError.
        """
        return ImportError(
            f'{format_name} {operation} support requires '
            f'"{module_name}" with {method_name}().',
        )

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
        return module_method(*args, **(dict(kwargs) if kwargs else {}))

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

        Raises
        ------
        ImportError
            Raised when the required module method is missing.
        """
        raise self.build_missing_module_method_error(
            format_name=self.format_name,
            module_name=self.module_name,
            method_name=method_name,
            operation=self.operation,
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
