"""
:mod:`etlplus.utils._imports` module.

Shared helpers for safe lazy imports.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from importlib import import_module
from importlib.util import find_spec
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DependencyImporter',
    'ImportRequirement',
    # Functions
    'build_dependency_error_message',
    'dependency_label',
    'import_package',
    'module_available',
    'normalize_dependency_names',
    'safe_module_available',
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

    # -- Internal Instance Methods -- #

    def _should_reraise_nested_import(
        self,
        exc: BaseException,
        *,
        module_name: str,
    ) -> bool:
        """Return whether one nested import error should be re-raised."""
        return (
            self.strict_missing_name
            and isinstance(exc, ImportError)
            and (missing_name := getattr(exc, 'name', None)) is not None
            and missing_name != module_name
        )

    # -- Instance Methods -- #

    def import_package(
        self,
        module_name: str,
        *,
        error_message: str,
    ) -> Any:
        """
        Import one package using this importer's cache and error policy.

        Parameters
        ----------
        module_name : str
            Module name to import.
        error_message : str
            Error message used when import fails.

        Returns
        -------
        Any
            The imported module.

        Raises
        ------
        self.import_exceptions
            Propagated when ``strict_missing_name`` is enabled and the importer
            fails on a different nested module name.
        self.error_type
            Raised with *error_message* when the configured import fails.
        """
        module_name = _clean_dependency_name(module_name)

        if module_name in self.cache:
            return self.cache[module_name]

        try:
            module = self.importer(module_name)
        except self.import_exceptions as exc:
            if self._should_reraise_nested_import(exc, module_name=module_name):
                raise
            raise self.error_type(error_message) from exc

        self.cache[module_name] = module
        return module

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
        return self.import_package(
            module_name,
            error_message=build_dependency_error_message(
                module_name,
                format_name=format_name,
                pip_name=pip_name,
                required=required,
            ),
        )


@dataclass(frozen=True, slots=True)
class ImportRequirement:
    """One optional import requirement."""

    # -- Instance Attributes -- #

    modules: tuple[str, ...]
    package: str
    extra: str | None = None

    # -- Instance Methods -- #

    def is_available(
        self,
        *,
        availability_checker: Callable[[str], bool],
    ) -> bool:
        """
        Return whether any module for this requirement is available.

        Parameters
        ----------
        availability_checker : Callable[[str], bool]
            Callable used to check module availability.

        Returns
        -------
        bool
            ``True`` when at least one required module is available.
        """
        return any(availability_checker(module_name) for module_name in self.modules)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _clean_dependency_name(
    value: str,
    *,
    label: str = 'module_name',
) -> str:
    """Return one stripped dependency name or raise a clear error."""
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f'{label} must not be empty')
    return cleaned


def _clean_dependency_names(
    values: tuple[str, ...],
    *,
    label: str = 'module_name',
) -> tuple[str, ...]:
    """Return stripped dependency names or raise a clear error."""
    return tuple(_clean_dependency_name(value, label=label) for value in values)


# SECTION: FUNCTIONS ======================================================== #


def dependency_label(
    dependency_names: tuple[str, ...],
) -> str:
    """
    Return one quoted dependency label string for an error message.

    Parameters
    ----------
    dependency_names : tuple[str, ...]
        One or more cleaned dependency names.

    Returns
    -------
    str
        Quoted dependency label string.

    Raises
    ------
    ValueError
        If *dependency_names* is empty.
    """
    if not dependency_names:
        raise ValueError('dependency_names must not be empty')
    cleaned_names = _clean_dependency_names(
        dependency_names,
        label='dependency name',
    )
    quoted = tuple(f'"{name}"' for name in cleaned_names)
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
    """
    Normalize dependency names and install target for message formatting.

    Parameters
    ----------
    module_name : DependencyNames
        One or more module names for the dependency.
    pip_name : str | None
        Optional package name to suggest for installation. Defaults to the
        first cleaned module name when ``None`` or invalid. Ignored when
        *module_name* is a string.

    Returns
    -------
    tuple[tuple[str, ...], str]
        Tuple of (normalized module names, normalized pip name or first module
        name).

    Raises
    ------
    ValueError
        If *module_name* is an empty tuple or any cleaned names are empty.
    """
    normalized_pip_name = (
        _clean_dependency_name(pip_name, label='pip_name')
        if pip_name is not None
        else None
    )
    if isinstance(module_name, str):
        cleaned_module_name = _clean_dependency_name(
            module_name,
            label='module_name',
        )
        dependency_display_name = normalized_pip_name or cleaned_module_name
        return (dependency_display_name,), dependency_display_name
    if not module_name:
        raise ValueError('module_name must not be an empty tuple')
    dependency_names = _clean_dependency_names(module_name, label='module_name')
    return dependency_names, normalized_pip_name or dependency_names[0]


def build_dependency_error_message(
    module_name: DependencyNames,
    format_name: str,
    pip_name: str | None = None,
    *,
    required: bool = False,
) -> str:
    """
    Build an import error message for one dependency.

    Parameters
    ----------
    module_name : DependencyNames
        One or more module names for the dependency.
    format_name : str
        Name of the format requiring the dependency.
    pip_name : str | None, optional
        Optional package name to suggest for installation. Defaults to the
        first cleaned module name when ``None`` or invalid. Ignored when
        *module_name* is a string.
    required : bool, optional
        Whether the dependency is required. Defaults to ``False``.

    Returns
    -------
    str
        Formatted error message indicating the missing dependency and how to
        install it.
    """
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
    """
    return DependencyImporter(
        cache=cache if cache is not None else {},
        error_type=error_type,
        import_exceptions=import_exceptions,
        importer=importer,
        strict_missing_name=strict_missing_name,
    ).import_package(
        module_name,
        error_message=error_message,
    )


def module_available(
    module_name: str,
    *,
    spec_finder: Callable[[str], object | None] = find_spec,
) -> bool:
    """
    Return whether *module_name* is importable without importing it.

    Parameters
    ----------
    module_name : str
        Module name to inspect.
    spec_finder : Callable[[str], object | None], optional
        Callable used to resolve import metadata. Defaults to
        :func:`importlib.util.find_spec`.

    Returns
    -------
    bool
        ``True`` when import metadata can resolve *module_name*.
    """
    try:
        normalized = _clean_dependency_name(module_name, label='module_name')
        return spec_finder(normalized) is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def safe_module_available(
    module_name: str,
    *,
    availability_checker: Callable[[str], bool] = module_available,
) -> bool:
    """
    Return whether *module_name* is available, treating checker errors as false.

    Parameters
    ----------
    module_name : str
        Module name to inspect.
    availability_checker : Callable[[str], bool], optional
        Callable used to check module availability. Defaults to
        :func:`module_available`.

    Returns
    -------
    bool
        ``True`` when the checker reports the module is available; ``False``
        when unavailable or when the checker raises an import/name error.
    """
    try:
        return availability_checker(module_name)
    except (ImportError, ModuleNotFoundError, ValueError):
        return False
