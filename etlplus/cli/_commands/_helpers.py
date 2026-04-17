"""
:mod:`etlplus.cli._commands._helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any
from typing import Final
from typing import Literal
from typing import NoReturn
from typing import overload

import typer

from ...file import FileFormat
from ...utils._data import parse_json
from ._constants import DATA_CONNECTORS
from ._constants import FILE_FORMATS
from ._state import CliState
from ._state import ResourceTypeResolver
from ._state import ensure_state
from ._state import resolve_logged_resource_type as _resolve_logged_resource_type
from ._types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'call_handler',
    'call_history_command',
    'call_history_handler',
    'fail_usage',
    'normalize_file_format',
    'parse_json_option',
    'require_any',
    'require_value',
    'resolve_command_resource',
    'resolve_resource',
]


# SECTION: TYPE ALIASES ===================================================== #


type _StateField = Literal['pretty', 'quiet', 'verbose']


# SECTION: INTERNAL CONSTANTS =============================================== #


_MISSING: Final[object] = object()


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _ResolvedResource:
    """Normalized source/target CLI inputs for one command invocation."""

    # -- Instance Attributes -- #

    value: str
    resource_type: str | None = None
    format_hint: FileFormat | None = None

    # -- Getters -- #

    @property
    def format_explicit(self) -> bool:
        """
        Return whether the resource carries one explicit format hint.

        This is used to determine whether to apply implicit format inference
        based on the value when the handler supports it.

        Returns
        -------
        bool
            ``True`` if the resource has an explicit format hint, ``False`` if
            not.
        """
        return self.format_hint is not None

    # -- Instance Methods -- #

    def require_resource_type(self) -> str:
        """
        Return the resolved connector type, asserting it is available.

        Returns
        -------
        str
            The resolved connector type.
        """
        assert self.resource_type is not None
        return self.resource_type


# SECTION: INTERNAL FUNCTIONS ============================================== #


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
    normalized = ResourceTypeResolver.optional_choice(
        None if value is None else str(value),
        choices,
        label=label,
    )
    if normalized is None or coerce is None:
        return normalized
    return coerce(normalized)


def _normalize_resource_type(
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


# SECTION: FUNCTIONS ======================================================== #


def call_handler(
    handler: Callable[..., int],
    /,
    *,
    state: CliState,
    state_fields: tuple[_StateField, ...] = ('pretty',),
    **kwargs: Any,
) -> int:
    """
    Invoke one handler with selected state-derived keyword arguments.

    Parameters
    ----------
    handler : Callable[..., int]
        The handler function to invoke.
    state : CliState
        The CLI state to pull keyword arguments from.
    state_fields : tuple[_StateField, ...], optional
        The fields of *state* to pass as keyword arguments to *handler*
        (defaults to ('pretty',)).
    **kwargs : Any
        Additional keyword arguments to pass to *handler*.

    Returns
    -------
    int
        The exit code returned by *handler*.
    """
    state_kwargs: dict[str, Any] = {
        field: getattr(state, field) for field in state_fields
    }
    return handler(**kwargs, **state_kwargs)


def call_history_handler(
    handler: Callable[..., int],
    /,
    *,
    state: CliState,
    level: str | object = _MISSING,
    job: str | None | object = _MISSING,
    pipeline: str | None | object = _MISSING,
    run_id: str | None | object = _MISSING,
    since: str | None | object = _MISSING,
    until: str | None | object = _MISSING,
    status: str | None | object = _MISSING,
    limit: int | None | object = _MISSING,
    **kwargs: Any,
) -> int:
    """
    Invoke one persisted-history handler with shared query filters.

    Parameters
    ----------
    handler : Callable[..., int]
        The handler function to invoke.
    state : CliState
        The CLI state to pull keyword arguments from.
    level : str | object, optional
        History level filter (defaults to missing, which means the handler's
        default will be used).
    job : str | None | object, optional
        Job name filter (defaults to missing, which means the handler's default
        will be used).
    pipeline : str | None | object, optional
        Pipeline name filter (defaults to missing, which means the handler's
        default will be used).
    run_id : str | None | object, optional
        Run ID filter (defaults to missing, which means the handler's default
        will be used).
    since : str | None | object, optional
        Start time filter (defaults to missing, which means the handler's default
        will be used).
    until : str | None | object, optional
        End time filter (defaults to missing, which means the handler's default
        will be used).
    status : str | None | object, optional
        Status filter (defaults to missing, which means the handler's default
        will be used).
    limit : int | None | object, optional
        Result limit (defaults to missing, which means the handler's default
        will be used).
    **kwargs : Any
        Additional keyword arguments to pass to *handler*.

    Returns
    -------
    int
        The exit code returned by *handler*.
    """
    history_kwargs = {
        key: value
        for key, value in {
            'level': level,
            'job': job,
            'limit': limit,
            'pipeline': pipeline,
            'run_id': run_id,
            'since': since,
            'status': status,
            'until': until,
        }.items()
        if value is not _MISSING
    }
    return handler(
        pretty=state.pretty,
        **history_kwargs,
        **kwargs,
    )


def call_history_command(
    handler: Callable[..., int],
    /,
    *,
    ctx: typer.Context,
    state: CliState | None = None,
    **kwargs: Any,
) -> int:
    """
    Invoke one history-style command using CLI state from *ctx* by default.

    Parameters
    ----------
    handler : Callable[..., int]
        The history-oriented handler function to invoke.
    ctx : typer.Context
        The Typer context used to resolve CLI state when *state* is not given.
    state : CliState | None, optional
        Existing CLI state to reuse (defaults to ``None``, which means
        :func:`ensure_state` will be called).
    **kwargs : Any
        Additional keyword arguments to forward through
        :func:`call_history_handler`.

    Returns
    -------
    int
        The exit code returned by *handler*.
    """
    return call_history_handler(
        handler,
        state=ensure_state(ctx) if state is None else state,
        **kwargs,
    )


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
        return parse_json(value)
    except ValueError as exc:
        raise typer.BadParameter(f'Invalid JSON for {flag}: {exc}') from exc


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


def require_value(
    value: str | None,
    *,
    message: str,
    positional_name: str | None = None,
) -> str:
    """
    Require one CLI value and optionally reject misplaced options.

    Parameters
    ----------
    value : str | None
        The value to check.
    message : str
        The error message to emit if the value is not provided.
    positional_name : str | None, optional
        The name of the positional argument for error messages (defaults to None).

    Returns
    -------
    str
        The validated value.
    """
    if not value:
        fail_usage(message)
    if positional_name is not None and value.startswith('--'):
        fail_usage(
            f"Option '{value}' must follow the '{positional_name}' argument.",
        )
    return value


def resolve_resource(
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
    """
    Return normalized value, connector type, and format for one resource.

    Parameters
    ----------
    state : CliState
        The CLI state to pull keyword arguments from for error reporting.
    role : DataConnectorContext
        The resource role for error messages ('source' or 'target').
    value : str | None
        The raw CLI value to resolve.
    connector_type : str | None, optional
        An explicit connector type to validate against known connectors
        (defaults to ``None``, which means no validation or normalization will
        be performed).
    format_value : FileFormat | str | None, optional
        An explicit file format hint to validate and normalize to
        :class:`FileFormat` (defaults to ``None``, which means no validation or
        normalization will be performed).
    positional : bool, optional
        Whether the value comes from a positional argument, which requires
        additional validation to reject option-like values (defaults to
        ``False``).
    soft_inference : bool, optional
        Whether to allow soft inference of the connector type based on the
        value when an explicit *connector_type* is not provided (defaults to
        ``False``, which means the type will be ``None`` when not explicitly
        provided).
    default_value : str, optional
        The default value to use when *value* is ``None`` (defaults to '-',
        which is a common CLI convention for STDIN/STDOUT).

    Returns
    -------
    _ResolvedResource
        The resolved resource components.
    """
    resolved_value = default_value if value is None else value
    if positional:
        resolved_value = require_value(
            resolved_value,
            message=f"Missing required argument '{role.upper()}'.",
            positional_name=role.upper(),
        )
    return _ResolvedResource(
        value=resolved_value,
        resource_type=_resolve_logged_resource_type(
            state,
            role=role,
            value=resolved_value,
            explicit_type=_normalize_resource_type(
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


def resolve_command_resource(
    ctx: typer.Context,
    *,
    role: DataConnectorContext,
    value: str | None,
    state: CliState | None = None,
    connector_type: str | None = None,
    format_value: FileFormat | str | None = None,
    positional: bool = False,
    soft_inference: bool = False,
    default_value: str = '-',
) -> tuple[CliState, _ResolvedResource]:
    """
    Return one CLI state plus one normalized resource for a command wrapper.

    Parameters
    ----------
    ctx : typer.Context
        Typer context used to initialize CLI state when *state* is not given.
    role : DataConnectorContext
        The resource role for error messages ('source' or 'target').
    value : str | None
        The raw CLI value to resolve.
    state : CliState | None, optional
        Existing CLI state to reuse (defaults to ``None``, which means
        :func:`ensure_state` will be called).
    connector_type : str | None, optional
        An explicit connector type override.
    format_value : FileFormat | str | None, optional
        An explicit file format override.
    positional : bool, optional
        Whether the value comes from a positional argument.
    soft_inference : bool, optional
        Whether to tolerate failed connector-type inference.
    default_value : str, optional
        The fallback CLI value to use when *value* is ``None``.

    Returns
    -------
    tuple[CliState, _ResolvedResource]
        The CLI state plus the normalized resource.
    """
    resolved_state = ensure_state(ctx) if state is None else state
    return resolved_state, resolve_resource(
        resolved_state,
        role=role,
        value=value,
        connector_type=connector_type,
        format_value=format_value,
        positional=positional,
        soft_inference=soft_inference,
        default_value=default_value,
    )
