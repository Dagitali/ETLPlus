"""
:mod:`etlplus.cli._commands.helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any
from typing import Literal
from typing import NoReturn
from typing import Protocol
from typing import overload

import typer

from ...file import FileFormat
from .._constants import DATA_CONNECTORS
from .._constants import FILE_FORMATS
from .._io import parse_json_payload
from .._state import CliState
from .._state import optional_choice as normalize_choice
from .._state import resolve_logged_resource_type  # noqa: F401
from .._types import DataConnectorContext

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


# SECTION: TYPE ALIASES ===================================================== #


type _StateField = Literal['pretty', 'quiet', 'verbose']


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _ResolvedResource:
    """Normalized source/target CLI inputs for one command invocation."""

    value: str
    resource_type: str | None = None
    format_hint: FileFormat | None = None


class _CliHandler(Protocol):
    """Protocol for CLI handlers that accept keyword arguments and return int."""

    def __call__(self, /, **kwargs: Any) -> int: ...


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _call_handler(
    handler: _CliHandler,
    /,
    *,
    state: CliState,
    state_fields: tuple[_StateField, ...] = ('pretty',),
    **kwargs: Any,
) -> int:
    """Invoke one handler with selected state-derived keyword arguments."""
    state_kwargs: dict[str, Any] = {
        field: getattr(state, field) for field in state_fields
    }
    return handler(**kwargs, **state_kwargs)


@overload
def _normalize_optional_choice(
    value: object | None,
    choices: Collection[str],
    *,
    label: str,
) -> str | None: ...


@overload
def _normalize_optional_choice[T](
    value: object | None,
    choices: Collection[str],
    *,
    label: str,
    coerce: Callable[[str], T],
) -> T | None: ...


def _normalize_optional_choice[T](
    value: object | None,
    choices: Collection[str],
    *,
    label: str,
    coerce: Callable[[str], T] | None = None,
) -> str | T | None:
    """Normalize one optional CLI value against *choices*."""
    normalized = normalize_choice(
        None if value is None else str(value),
        choices,
        label=label,
    )
    if normalized is None or coerce is None:
        return normalized
    return coerce(normalized)


def _resolve_resource(
    state: CliState,
    *,
    role: DataConnectorContext,
    value: str | None,
    connector_type: str | None = None,
    format_value: FileFormat | str | None = None,
    positional: bool = False,
    soft_inference: bool = False,
    default_value: str = '-',
) -> _ResolvedResource:
    """Return normalized value, connector type, and format for one resource."""
    resolved_value = default_value if value is None else value
    if positional:
        resolved_value = require_positional_argument(
            resolved_value,
            name=role.upper(),
        )
    return _ResolvedResource(
        value=resolved_value,
        resource_type=resolve_logged_resource_type(
            state,
            role=role,
            value=resolved_value,
            explicit_type=normalize_resource_type(
                connector_type,
                label=f'{role}_type',
            ),
            soft_inference=soft_inference,
        ),
        format_hint=normalize_file_format(
            format_value,
            label=f'{role}_format',
        ),
    )


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
    return _normalize_optional_choice(
        value,
        FILE_FORMATS,
        label=label,
        coerce=FileFormat.coerce,
    )


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
    return _normalize_optional_choice(
        value,
        DATA_CONNECTORS,
        label=label,
    )


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
