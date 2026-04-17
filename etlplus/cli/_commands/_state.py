"""
:mod:`etlplus.cli._commands._state` module.

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

from ...utils import normalize_str
from ._constants import DATA_CONNECTORS
from ._types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #

__all__ = [
    # Classes
    'CliState',
    'ResourceTypeResolver',
    # Functions
    'ensure_state',
    'log_inferred_resource',
    'resolve_logged_resource_type',
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
            return cls.infer(value)
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
        return cls.validate(value, choices, label=label)

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
            candidate = override_type or cls.infer_or_exit(value)
        return cls.validate(candidate, DATA_CONNECTORS, label=label)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _set_state(
    ctx: typer.Context,
    *,
    pretty: bool,
    quiet: bool,
    verbose: bool,
) -> CliState:
    """Replace the context state with one explicit CLI root configuration."""
    state = CliState(pretty=pretty, quiet=quiet, verbose=verbose)
    ctx.obj = state
    return state


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


def log_inferred_resource(
    state: CliState,
    *,
    role: DataConnectorContext,
    value: str,
    resource_type: str | None,
) -> None:
    """
    Emit a uniform verbose message for inferred resource types.

    Parameters
    ----------
    state : CliState
        The current CLI state.
    role : DataConnectorContext
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


def resolve_logged_resource_type(
    state: CliState,
    *,
    role: DataConnectorContext,
    value: str,
    explicit_type: str | None,
    soft_inference: bool = False,
) -> str | None:
    """
    Resolve one resource type, validate it, and emit shared verbose logging.

    Parameters
    ----------
    state : CliState
        The current CLI state.
    role : DataConnectorContext
        The resource role, e.g. ``'source'`` or ``'target'``.
    value : str
        The resource identifier (path, URL, or DSN).
    explicit_type : str | None
        The explicit CLI connector type, if provided.
    soft_inference : bool, optional
        Whether to tolerate inference failures and return ``None``.

    Returns
    -------
    str | None
        The validated resource type, or ``None`` when soft inference fails.
    """
    resource_type = explicit_type
    if resource_type is None:
        infer = (
            ResourceTypeResolver.infer_soft
            if soft_inference
            else ResourceTypeResolver.infer_or_exit
        )
        resource_type = infer(value)
    if resource_type is not None:
        resource_type = ResourceTypeResolver.validate(
            resource_type,
            DATA_CONNECTORS,
            label=f'{role}_type',
        )
    log_inferred_resource(
        state,
        role=role,
        value=value,
        resource_type=resource_type,
    )
    return resource_type
