"""
:mod:`etlplus.utils._imports` module.

Shared helpers for safe lazy imports.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from importlib import import_module
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DependencyImporter',
    # Functions
    'build_dependency_error_message',
    'dependency_label',
    'import_package',
    'normalize_dependency_names',
    # Type Aliases
    'DependencyNames',
]


# SECTION: TYPE ALIASES ===================================================== #


type DependencyNames = str | tuple[str, ...]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class DependencyImporter:
    """Configurable cached dependency importer."""

    # -- Instance Attributes -- #

    error_type: Callable[[str], Exception] = ImportError
    import_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (
        ImportError
    )
    strict_missing_name: bool = False
    importer: Callable[[str], Any] = import_module
    cache: dict[str, Any] = field(default_factory=dict)

    # -- Instance Methods -- #

    def get(
        self,
        module_name: str,
        *,
        format_name: str,
        pip_name: str | None = None,
        required: bool = False,
    ) -> Any:
        """
        Import one dependency module using this import policy.

        Parameters
        ----------
        module_name : str
            Name of the module to import.
        format_name : str
            Human-readable format name for error messages.
        pip_name : str | None, optional
            Package name to suggest for installation (defaults to
            *module_name*).
        required : bool, optional
            Whether to use required-dependency message wording. Defaults to
            ``False`` (optional dependency wording).

        Returns
        -------
        Any
            The imported module.
        """
        return import_package(
            module_name,
            error_message=build_dependency_error_message(
                module_name,
                format_name=format_name,
                pip_name=pip_name,
                required=required,
            ),
            cache=self.cache,
            importer=self.importer,
            error_type=self.error_type,
            import_exceptions=self.import_exceptions,
            strict_missing_name=self.strict_missing_name,
        )


# SECTION: FUNCTIONS ======================================================== #


def dependency_label(
    dependency_names: tuple[str, ...],
) -> str:
    """Return one quoted dependency label string for an error message."""
    if not dependency_names:
        raise ValueError('dependency_names must not be empty')
    quoted = tuple(f'"{name}"' for name in dependency_names)
    if len(quoted) == 1:
        return quoted[0]
    if len(quoted) == 2:
        first, second = quoted
        return f'{first} or {second}'
    return f'{", ".join(quoted[:-1])}, or {quoted[-1]}'


def normalize_dependency_names(
    module_name: DependencyNames,
    pip_name: str | None,
) -> tuple[tuple[str, ...], str]:
    """Normalize dependency names and install target for message formatting."""
    if isinstance(module_name, str):
        dependency_display_name = pip_name or module_name
        return (dependency_display_name,), dependency_display_name
    if not module_name:
        raise ValueError('module_name must not be an empty tuple')
    return module_name, pip_name or module_name[0]


def build_dependency_error_message(
    module_name: DependencyNames,
    format_name: str,
    pip_name: str | None = None,
    *,
    required: bool = False,
) -> str:
    """Build an import error message for one dependency."""
    dependency_names, dependency_target = normalize_dependency_names(
        module_name,
        pip_name,
    )
    label = 'dependency' if required else 'optional dependency'
    return (
        f'{format_name} support requires '
        f'{label} {dependency_label(dependency_names)}.\n'
        f'Install with: pip install {dependency_target}'
    )


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

    Raises
    ------
    import_exceptions
        Propagated when ``strict_missing_name`` is enabled and the importer
        fails on a different nested module name.
    error_type
        Raised with *error_message* when the configured import fails.

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
                raise
        raise error_type(error_message) from exc

    if cache is not None:
        cache[module_name] = module
    return module
