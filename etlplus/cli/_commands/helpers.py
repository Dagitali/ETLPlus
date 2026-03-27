"""
:mod:`etlplus.cli._commands.helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any
from typing import Literal
from typing import NoReturn
from typing import cast

import typer

from ...file import FileFormat
from .._constants import DATA_CONNECTORS
from .._constants import FILE_FORMATS
from .._io import parse_json_payload
from .._state import CliState
from .._state import ResourceTypeResolver
from .._state import log_inferred_resource
from .._state import validate_choice

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'fail_usage',
    'normalize_choice',
    'parse_json_option',
    'reject_option_like_argument',
    'require_any',
    'require_option',
    'require_positional_argument',
]


# SECTION: INTERNAL TYPE ALIASES ============================================ #


type _ResourceRole = Literal['source', 'target']


# SECTION: FUNCTIONS ======================================================== #


def fail_usage(
    message: str,
    *,
    exit_code: int = 2,
) -> NoReturn:
    """
    Emit one CLI usage error message and abort with *exit_code*.

    Parameters
    ----------
    message : str
        The error message to emit.
    exit_code : int, optional
        The exit code to abort with (defaults to 2, the standard for CLI usage
        errors).

    Raises
    ------
    typer.Exit
        Always raises to abort the CLI command.
    """
    typer.echo(f'Error: {message}', err=True)
    raise typer.Exit(exit_code)


def normalize_choice(
    value: str | None,
    choices: Collection[str],
    *,
    label: str,
) -> str | None:
    """
    Validate an optional CLI choice while preserving ``None``.

    Parameters
    ----------
    value : str | None
        The value to validate.
    choices : Collection[str]
        The valid choices.
    label : str
        The label for error messages.

    Returns
    -------
    str | None
        The validated value or ``None`` if not provided.
    """
    return ResourceTypeResolver.optional_choice(value, choices, label=label)


def normalize_file_format(
    value: FileFormat | str | None,
    *,
    label: str,
) -> FileFormat | None:
    """
    Normalize optional file-format CLI values to :class:`FileFormat`.

    Parameters
    ----------
    value : FileFormat | str | None
        The value to normalize.
    label : str
        The label for error messages.

    Returns
    -------
    FileFormat | None
        The normalized file format or ``None`` if not provided.
    """
    normalized = normalize_choice(
        None if value is None else str(value),
        FILE_FORMATS,
        label=label,
    )
    if normalized is None:
        return None
    return FileFormat.coerce(normalized)


def normalize_resource_type(
    value: str | None,
    *,
    label: str,
) -> str | None:
    """
    Normalize optional connector types against known CLI connectors.

    Parameters
    ----------
    value : str | None
        The value to normalize.
    label : str
        The label for error messages.

    Returns
    -------
    str | None
        The normalized connector type or ``None`` if not provided.
    """
    return normalize_choice(value, DATA_CONNECTORS, label=label)


def parse_json_option(
    value: str,
    flag: str,
) -> Any:
    """
    Parse JSON option values and surface a helpful CLI error.

    Parameters
    ----------
    value : str
        The JSON string to parse.
    flag : str
        The CLI flag name for error messages.

    Returns
    -------
    Any
        The parsed JSON value.

    Raises
    ------
    typer.BadParameter
        When the JSON is invalid.
    """
    try:
        return parse_json_payload(value)
    except ValueError as exc:
        raise typer.BadParameter(f'Invalid JSON for {flag}: {exc}') from exc


def reject_option_like_argument(
    value: str,
    *,
    name: str,
) -> str:
    """
    Reject option-like positional values so usage errors stay explicit.

    Parameters
    ----------
    value : str
        The value to validate.
    name : str
        The name of the positional argument.

    Returns
    -------
    str
        The validated value.
    """
    if value.startswith('--'):
        fail_usage(f"Option '{value}' must follow the '{name}' argument.")
    return value


def require_any(
    values: Collection[object],
    *,
    message: str,
) -> None:
    """
    Require at least one truthy value from *values*.

    Parameters
    ----------
    values : Collection[object]
        The values to check.
    message : str
        The error message to emit if none of the values are truthy.
    """
    if not any(values):
        fail_usage(message)


def require_option(
    value: str | None,
    *,
    flag: str,
) -> str:
    """
    Require one CLI option value and return it when present.

    Parameters
    ----------
    value : str | None
        The value to check.
    flag : str
        The CLI flag name for error messages.

    Returns
    -------
    str
        The validated value.
    """
    if not value:
        fail_usage(f"Missing required option '{flag}'.")
    return value


def require_positional_argument(
    value: str,
    *,
    name: str,
) -> str:
    """
    Require one positional argument and reject misplaced options.

    Parameters
    ----------
    value : str
        The value to check.
    name : str
        The name of the positional argument.

    Returns
    -------
    str
        The validated value.
    """
    if not value:
        fail_usage(f"Missing required argument '{name}'.")
    return reject_option_like_argument(value, name=name)


def resolve_logged_resource_type(
    state: CliState,
    *,
    role: _ResourceRole,
    value: str,
    explicit_type: str | None,
    soft_inference: bool = False,
) -> str | None:
    """
    Resolve one resource type and emit the shared verbose inference log.

    Parameters
    ----------
    state : CliState
        The CLI state.
    role : _ResourceRole
        The role of the resource.
    value : str
        The value to resolve.
    explicit_type : str | None
        The explicit type, if provided.
    soft_inference : bool, optional
        Whether to use soft inference, by default False.

    Returns
    -------
    str | None
        The resolved resource type or ``None`` if not determined.
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
        resource_type = validate_choice(
            resource_type,
            DATA_CONNECTORS,
            label=f'{role}_type',
        )
    log_inferred_resource(
        state,
        role=cast(str, role),
        value=value,
        resource_type=resource_type,
    )
    return resource_type
