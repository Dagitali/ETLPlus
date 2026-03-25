"""
:mod:`etlplus.cli.state` module.

Shared state and helper utilities for the ``etlplus`` command-line interface
(CLI).
"""

from __future__ import annotations

import sys
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer

from ..utils import normalize_str
from ._constants import DATA_CONNECTORS

# SECTION: EXPORTS ========================================================== #

__all__ = [
    # Classes
    'CliState',
    'ResourceTypeResolver',
    # Functions
    'ensure_state',
    'infer_resource_type',
    'infer_resource_type_or_exit',
    'infer_resource_type_soft',
    'log_inferred_resource',
    'optional_choice',
    'resolve_resource_type',
    'validate_choice',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_DB_SCHEMES: Final[tuple[str, ...]] = (
    'postgres://',
    'postgresql://',
    'mysql://',
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class CliState:
    """
    Mutable container for runtime CLI toggles.

    Attributes
    ----------
    pretty : bool
        Whether to pretty-print output.
    quiet : bool
        Whether to suppress non-error output.
    verbose : bool
        Whether to enable verbose logging.
    """

    pretty: bool = True
    quiet: bool = False
    verbose: bool = False


# SECTION: CLASSES ========================================================== #


class ResourceTypeResolver:
    """Shared normalization and inference helpers for CLI resource types."""

    # -- Static Methods -- #

    @staticmethod
    def infer(
        value: str,
    ) -> str:
        """Infer the resource type from a path, URL, or DSN string."""
        val = (value or '').strip()
        low = val.lower()

        match (val, low):
            case ('-', _):
                return 'file'
            case (_, inferred) if inferred.startswith(('http://', 'https://')):
                return 'api'
            case (_, inferred) if inferred.startswith(_DB_SCHEMES):
                return 'database'

        path = Path(val)
        if path.exists() or path.suffix:
            return 'file'

        raise ValueError(
            (
                'Could not infer resource type. '
                'Use --source-type/--target-type to specify it.'
            ),
        )

    @staticmethod
    def infer_or_exit(
        value: str,
    ) -> str:
        """Infer a resource type and map ``ValueError`` to ``BadParameter``."""
        try:
            return ResourceTypeResolver.infer(value)
        except ValueError as exc:  # pragma: no cover - exercised indirectly
            raise typer.BadParameter(str(exc)) from exc

    @staticmethod
    def validate(
        value: str | object,
        choices: Collection[str],
        *,
        label: str,
    ) -> str:
        """Validate CLI input against a whitelist of choices."""
        normalized_value = normalize_str(str(value or ''))
        normalized_choices = {normalize_str(choice): choice for choice in choices}
        if normalized_value in normalized_choices:
            return normalized_choices[normalized_value]
        allowed = ', '.join(sorted(choices))
        raise typer.BadParameter(
            f"Invalid {label} '{value}'. Choose from: {allowed}",
        )

    # -- Class Methods -- #

    @classmethod
    def infer_soft(
        cls,
        value: str | None,
    ) -> str | None:
        """Make a best-effort inference that tolerates inline payloads."""
        if value is None:
            return None
        try:
            return infer_resource_type(value)
        except ValueError:
            return None

    @classmethod
    def optional_choice(
        cls,
        value: str | None,
        choices: Collection[str],
        *,
        label: str,
    ) -> str | None:
        """Validate optional CLI choice inputs while preserving ``None``."""
        if value is None:
            return None
        return validate_choice(value, choices, label=label)

    @classmethod
    def resolve(
        cls,
        *,
        explicit_type: str | None,
        override_type: str | None,
        value: str,
        label: str,
        conflict_error: str | None = None,
        legacy_file_error: str | None = None,
    ) -> str:
        """Resolve resource type preference order and validate it."""
        if explicit_type is not None:
            if override_type is not None and conflict_error:
                raise typer.BadParameter(conflict_error)
            if legacy_file_error and explicit_type.strip().lower() == 'file':
                raise typer.BadParameter(legacy_file_error)
            candidate = explicit_type
        else:
            candidate = override_type or infer_resource_type_or_exit(value)
        return validate_choice(candidate, DATA_CONNECTORS, label=label)


# SECTION: FUNCTIONS ======================================================== #


def ensure_state(
    ctx: typer.Context,
) -> CliState:
    """
    Return the :class:`CliState` stored on the :mod:`typer` context.

    Initializes a new :class:`CliState` if none exists.

    Parameters
    ----------
    ctx : typer.Context
        The Typer command context.

    Returns
    -------
    CliState
        The CLI state object.
    """
    if not isinstance(getattr(ctx, 'obj', None), CliState):
        ctx.obj = CliState()
    return ctx.obj


def infer_resource_type(
    value: str,
) -> str:
    """
    Infer the resource type from a path, URL, or DSN string.

    Parameters
    ----------
    value : str
        The resource identifier (path, URL, or DSN).

    Returns
    -------
    str
        The inferred resource type: ``file``, ``api``, or ``database``.

    """
    return ResourceTypeResolver.infer(value)


def infer_resource_type_or_exit(
    value: str,
) -> str:
    """
    Infer a resource type and map ``ValueError`` to ``BadParameter``.

    Parameters
    ----------
    value : str
        The resource identifier (path, URL, or DSN).

    Returns
    -------
    str
        The inferred resource type: ``file``, ``api``, or ``database``.

    """
    return ResourceTypeResolver.infer_or_exit(value)


def infer_resource_type_soft(
    value: str | None,
) -> str | None:
    """
    Make a best-effort inference that tolerates inline payloads.

    Parameters
    ----------
    value : str | None
        The resource identifier (path, URL, DSN, or inline payload).

    Returns
    -------
    str | None
        The inferred resource type, or ``None`` if inference failed.
    """
    return ResourceTypeResolver.infer_soft(value)


def log_inferred_resource(
    state: CliState,
    *,
    role: str,
    value: str,
    resource_type: str | None,
) -> None:
    """
    Emit a uniform verbose message for inferred resource types.

    Parameters
    ----------
    state : CliState
        The current CLI state.
    role : str
        The resource role, e.g., ``source`` or ``target``.
    value : str
        The resource identifier (path, URL, or DSN).
    resource_type : str | None
        The inferred resource type, or ``None`` if inference failed.
    """
    if state.quiet or not state.verbose or resource_type is None:
        return
    print(
        f'Inferred {role}_type={resource_type} for {role}={value}',
        file=sys.stderr,
    )


def optional_choice(
    value: str | None,
    choices: Collection[str],
    *,
    label: str,
) -> str | None:
    """
    Validate optional CLI choice inputs while preserving ``None``.

    Parameters
    ----------
    value : str | None
        The input value to validate, or ``None``.
    choices : Collection[str]
        The set of valid choices.
    label : str
        The label for error messages.

    Returns
    -------
    str | None
        The validated choice, or ``None`` if input was ``None``.
    """
    return ResourceTypeResolver.optional_choice(value, choices, label=label)


def resolve_resource_type(
    *,
    explicit_type: str | None,
    override_type: str | None,
    value: str,
    label: str,
    conflict_error: str | None = None,
    legacy_file_error: str | None = None,
) -> str:
    """
    Resolve resource type preference order and validate it.

    Parameters
    ----------
    explicit_type : str | None
        The explicit resource type from the CLI, or ``None`` if not provided.
    override_type : str | None
        The override resource type from the CLI, or ``None`` if not provided.
    value : str
        The resource identifier (path, URL, or DSN).
    label : str
        The label for error messages.
    conflict_error : str | None, optional
        The error message to raise if both explicit and override types are
        provided, by default ``None``.
    legacy_file_error : str | None, optional
        The error message to raise if the explicit type is ``file``, by default
        ``None``.

    Returns
    -------
    str
        The resolved and validated resource type.

    """
    return ResourceTypeResolver.resolve(
        explicit_type=explicit_type,
        override_type=override_type,
        value=value,
        label=label,
        conflict_error=conflict_error,
        legacy_file_error=legacy_file_error,
    )


def validate_choice(
    value: str | object,
    choices: Collection[str],
    *,
    label: str,
) -> str:
    """
    Validate CLI input against a whitelist of choices.

    Parameters
    ----------
    value : str | object
        The input value to validate.
    choices : Collection[str]
        The set of valid choices.
    label : str
        The label for error messages.

    Returns
    -------
    str
        The validated choice.

    """
    return ResourceTypeResolver.validate(value, choices, label=label)
