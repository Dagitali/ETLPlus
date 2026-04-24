"""
:mod:`etlplus.file._imports` module.

Internal dependency import helpers for :mod:`file` modules.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from importlib import import_module
from typing import Any
from typing import ClassVar
from typing import NoReturn

from ..utils._imports import build_dependency_error_message
from ..utils._imports import dependency_label
from ..utils._imports import import_package
from ..utils._imports import normalize_dependency_names

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FormatDependencyResolverMixin',
    'FormatPandasResolverMixin',
    # Functions
    'get_dependency',
    'get_pandas',
    'get_pyarrow',
    'get_yaml',
    'raise_engine_import_error',
    'resolve_dependency',
    'resolve_module_callable',
    'resolve_pandas',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


# Dependency module support (lazy-loaded to avoid hard dependency)
_MODULE_CACHE: dict[str, Any] = {}

# SECTION: INTERNAL FUNCTIONS =============================================== #


_dependency_label = dependency_label


_error_message = build_dependency_error_message


_normalize_dependency_names = normalize_dependency_names


def _resolve_with_module_override(
    handler: object,
    override_name: str,
    fallback: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Resolve one dependency call via module override with fallback.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    override_name : str
        Callable name to resolve from the concrete module.
    fallback : Callable[..., Any]
        Fallback resolver when no override callable is present.
    *args : Any
        Positional arguments forwarded to the resolver.
    **kwargs : Any
        Keyword arguments forwarded to the resolver.

    Returns
    -------
    Any
        The resolved dependency/module value.
    """
    if resolver := resolve_module_callable(handler, override_name):
        return resolver(*args, **kwargs)
    return fallback(*args, **kwargs)


# SECTION: FUNCTIONS ======================================================== #


def get_dependency(
    module_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
    required: bool = False,
) -> Any:
    """
    Return a dependency module with a standardized error message.

    Parameters
    ----------
    module_name : str
        Name of the module to import.
    format_name : str
        Human-readable format name for error messages.
    pip_name : str | None, optional
        Package name to suggest for installation (defaults to *module_name*).
    required : bool, optional
        Whether to use required-dependency message wording.
        Defaults to ``False`` (optional dependency wording).

    Returns
    -------
    Any
        The imported module.

    """
    error_message = _error_message(
        module_name,
        format_name=format_name,
        pip_name=pip_name,
        required=required,
    )
    return import_package(
        module_name,
        error_message=error_message,
        cache=_MODULE_CACHE,
        importer=import_module,
        error_type=ImportError,
        import_exceptions=ImportError,
        strict_missing_name=True,
    )


def get_pandas(
    format_name: str,
) -> Any:
    """
    Return the pandas module, importing it on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The pandas module.
    """
    return get_dependency('pandas', format_name=format_name, required=True)


def get_pyarrow(
    format_name: str,
) -> Any:
    """
    Return the pyarrow module, importing it on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The pyarrow module.
    """
    return get_dependency('pyarrow', format_name=format_name, required=True)


def get_yaml() -> Any:
    """
    Return the required PyYAML module, importing it on first use.

    Returns
    -------
    Any
        The PyYAML module.
    """
    return get_dependency(
        'yaml',
        format_name='YAML',
        pip_name='PyYAML',
        required=True,
    )


def raise_engine_import_error(
    error: ImportError,
    *,
    format_name: str,
    dependency_names: str | tuple[str, ...] | None = None,
    pip_name: str | None = None,
    required: bool = False,
) -> NoReturn:
    """
    Raise one shared engine-dependency ImportError for a format.

    Parameters
    ----------
    error : ImportError
        Original engine import error.
    format_name : str
        Human-readable format name.
    dependency_names : str | tuple[str, ...] | None, optional
        One dependency name or tuple of alternative names used to build a
        standardized message. If None, re-raises *error*.
    pip_name : str | None, optional
        Package name hint for install command.
    required : bool, optional
        Whether to use required-dependency wording.

    Raises
    ------
    ImportError
        Standardized engine dependency message when dependency names are
        provided.
    error
        The original ImportError when dependency names are not provided.
    """
    if dependency_names is None:
        raise error
    message = _error_message(
        dependency_names,
        format_name=format_name,
        pip_name=pip_name,
        required=required,
    )
    raise ImportError(message) from error


def resolve_dependency(
    handler: object,
    dependency_name: str,
    *,
    format_name: str,
    pip_name: str | None = None,
    required: bool = False,
) -> Any:
    """
    Resolve one dependency with module-level override support.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    dependency_name : str
        Dependency import name.
    format_name : str
        Human-readable format name used for import error context.
    pip_name : str | None, optional
        Optional package name hint.
    required : bool, optional
        Whether the dependency should be treated as required when building
        import-error context. Defaults to ``False``.

    Returns
    -------
    Any
        The resolved dependency module.
    """
    return _resolve_with_module_override(
        handler,
        'get_dependency',
        get_dependency,
        dependency_name,
        format_name=format_name,
        pip_name=pip_name,
        required=required,
    )


def resolve_module_callable(
    handler: object,
    name: str,
) -> Callable[..., Any] | None:
    """
    Resolve one callable from the concrete handler module when present.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module should be inspected.
    name : str
        The callable name to resolve from the concrete module.

    Returns
    -------
    Callable[..., Any] | None
        The resolved callable if present and callable; otherwise ``None``.
    """
    module = sys.modules.get(type(handler).__module__)
    if module is None:
        return None
    value = getattr(module, name, None)
    return value if callable(value) else None


def resolve_pandas(
    handler: object,
    *,
    format_name: str,
) -> Any:
    """
    Resolve pandas with module-level override support.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    format_name : str
        Human-readable format name used for import error context.

    Returns
    -------
    Any
        The resolved pandas module.
    """
    return _resolve_with_module_override(
        handler,
        'get_pandas',
        get_pandas,
        format_name,
    )


# SECTION: CLASSES ========================================================== #


class FormatDependencyResolverMixin:
    """Shared dependency resolver keyed by :attr:`format_name`."""

    # -- Class Attributes -- #

    format_name: ClassVar[str]

    # -- Instance Methods -- #

    def resolve_format_dependency(
        self,
        dependency_name: str,
        *,
        pip_name: str | None = None,
    ) -> Any:
        """Resolve one dependency for this handler's format context."""
        return resolve_dependency(
            self,
            dependency_name,
            format_name=self.format_name,
            pip_name=pip_name,
        )


class FormatPandasResolverMixin(FormatDependencyResolverMixin):
    """Shared pandas resolver keyed by :attr:`format_name`."""

    # -- Instance Methods -- #

    def resolve_pandas(self) -> Any:
        """Return pandas using module-level override support."""
        return resolve_pandas(self, format_name=self.format_name)
