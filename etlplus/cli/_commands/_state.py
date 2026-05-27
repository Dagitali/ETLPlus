"""
:mod:`etlplus.cli._commands._state` module.

Shared CLI state and resource-type resolution for the ``etlplus`` command-line
interface.
"""

from __future__ import annotations

import sys
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import typer

from ...utils import TextNormalizer
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
        """Infer the resource type from a path, URL, or DSN string.

        Parameters
        ----------
        value : str
            The resource identifier (path, URL, or DSN) to infer the type of.

        Returns
        -------
        str
            The inferred resource type.

        Raises
        ------
        ValueError
            If the resource type cannot be inferred.
        """
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
    def validate(
        value: str | object,
        choices: Collection[str],
        *,
        label: str,
    ) -> str:
        """Validate CLI input against a whitelist of choices.

        Parameters
        ----------
        value : str | object
            The value to validate.
        choices : Collection[str]
            The set of valid choices.
        label : str
            The label for the value being validated.

        Returns
        -------
        str
            The validated value.

        Raises
        ------
        typer.BadParameter
            If the value is not in the set of valid choices.
        """
        normalized_value = TextNormalizer.normalize(str(value))
        normalized_choices = {
            TextNormalizer.normalize(choice): choice for choice in choices
        }
        if normalized_value in normalized_choices:
            return normalized_choices[normalized_value]
        allowed = ', '.join(sorted(choices))
        raise typer.BadParameter(
            f"Invalid {label} '{value}'. Choose from: {allowed}",
        )


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
        if soft_inference:
            try:
                resource_type = ResourceTypeResolver.infer(value)
            except ValueError:
                resource_type = None
        else:
            try:
                resource_type = ResourceTypeResolver.infer(value)
            except ValueError as exc:  # pragma: no cover - exercised indirectly
                raise typer.BadParameter(str(exc)) from exc
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
