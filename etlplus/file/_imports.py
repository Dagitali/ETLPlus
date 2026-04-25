"""
:mod:`etlplus.file._imports` module.

Internal dependency import helpers for :mod:`file` modules.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any
from typing import ClassVar
from typing import NoReturn

from ..utils._imports import DependencyImporter
from ..utils._imports import build_dependency_error_message

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
_DEPENDENCY_IMPORTER = DependencyImporter(
    strict_missing_name=True,
)

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _resolve_with_module_override(
    handler: object,
    override_name: str,
    fallback: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Resolve one module override or fall back."""
    return (resolve_module_callable(handler, override_name) or fallback)(
        *args,
        **kwargs,
    )


# SECTION: FUNCTIONS ======================================================== #


get_dependency = _DEPENDENCY_IMPORTER.get


def get_pandas(
    format_name: str,
) -> Any:
    """
    Return :mod:`pandas` lazily, importing on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The :mod:`pandas` module.
    """
    return get_dependency('pandas', format_name=format_name, required=True)


def get_pyarrow(
    format_name: str,
) -> Any:
    """
    Return :mod:`pyarrow` lazily, importing on first use.

    Parameters
    ----------
    format_name : str
        Human-readable format name for error messages.

    Returns
    -------
    Any
        The :mod:`pyarrow` module.
    """
    return get_dependency('pyarrow', format_name=format_name, required=True)


def get_yaml() -> Any:
    """
    Return :mod:`yaml` lazily, importing on first use.

    Returns
    -------
    Any
        The :mod:`yaml` module.
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
    message = build_dependency_error_message(
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
    Return a callable override from the handler module when present.

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
    Resolve :mod:`pandas` with module-level override support.

    Parameters
    ----------
    handler : object
        The handler instance whose concrete module may override resolution.
    format_name : str
        Human-readable format name used for import error context.

    Returns
    -------
    Any
        The resolved :mod:`pandas` module.
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
