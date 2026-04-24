"""
:mod:`etlplus.utils._imports` module.

Shared helpers for safe lazy imports.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any
from typing import NoReturn

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'import_package',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _raise_import_failure(
    error_factory: Callable[[str], Exception],
    error_message: str,
    error: BaseException,
) -> NoReturn:
    """Raise one wrapped dependency import error."""
    raise error_factory(error_message) from error


def _reraise(
    error: BaseException,
) -> NoReturn:
    """Re-raise one original import error."""
    raise error


# SECTION: FUNCTIONS ======================================================== #


def import_package(
    module_name: str,
    *,
    error_message: str,
    cache: dict[str, Any] | None = None,
    importer: Callable[[str], Any] = import_module,
    error_type: Callable[[str], Exception] = ImportError,
    import_exceptions: type[BaseException]
    | tuple[type[BaseException], ...] = ImportError,
    strict_missing_name: bool = False,
) -> Any:
    """
    Import one Python package with optional caching and error translation.

    Parameters
    ----------
    module_name : str
        Module name to import.
    error_message : str
        Error message used when import fails.
    cache : dict[str, Any] | None, optional
        Optional cache keyed by module name. Defaults to ``None``.
    importer : Callable[[str], Any], optional
        Import callable used to resolve the module. Defaults to
        :func:`importlib.import_module`.
    error_type : Callable[[str], Exception], optional
        Exception factory used to wrap import failures. Defaults to
        :class:`ImportError`.
    import_exceptions : type[BaseException] | tuple[type[BaseException], ...], optional
        Exception type or types to catch from *importer*. Defaults to
        :class:`ImportError`.
    strict_missing_name : bool, optional
        When ``True``, nested :class:`ImportError` failures whose ``name`` does
        not match *module_name* are re-raised unchanged. Defaults to ``False``.

    Returns
    -------
    Any
        The imported module.

    """
    if cache is not None:
        try:
            return cache[module_name]
        except KeyError:
            pass

    try:
        module = importer(module_name)
    except import_exceptions as exc:
        if strict_missing_name and isinstance(exc, ImportError):
            missing_name = getattr(exc, 'name', None)
            if missing_name is not None and missing_name != module_name:
                _reraise(exc)
        _raise_import_failure(error_type, error_message, exc)

    if cache is not None:
        cache[module_name] = module
    return module
