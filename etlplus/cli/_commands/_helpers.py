"""
:mod:`etlplus.cli._commands._helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import Final
from typing import Literal
from typing import NoReturn

import typer

from ...file import FileFormat
from ...utils import JsonCodec
from ._constants import DATA_CONNECTORS
from ._constants import FILE_FORMATS
from ._state import CliState
from ._state import ResourceTypeResolver
from ._state import ensure_state
from ._state import resolve_logged_resource_type as _resolve_logged_resource_type
from ._types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CommandHelperPolicy',
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
        Return whether the resource carries an explicit format hint.

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
        Return the resolved connector type, failing if it is unavailable.

        Returns
        -------
        str
            The resolved connector type.

        Raises
        ------
        ValueError
            If no connector type was resolved.
        """
        if self.resource_type is None:
            raise ValueError(f'No connector type resolved for {self.value!r}')
        return self.resource_type


# SECTION: CLASSES ========================================================== #


class CommandHelperPolicy:
    """Own shared command dispatch, validation, and resource normalization."""

    @staticmethod
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

    @staticmethod
    def call_history_command(
        handler: Callable[..., int],
        /,
        *,
        ctx: typer.Context,
        state: CliState | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Invoke one history-style command using CLI state from *ctx* by
        default.

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
            Additional keyword arguments to pass to *handler*. Missing history
            filter values are omitted so the handler's own defaults still
            apply.

        Returns
        -------
        int
            The exit code returned by *handler*.
        """
        history_state = ensure_state(ctx) if state is None else state
        history_kwargs = {
            key: value for key, value in kwargs.items() if value is not _MISSING
        }
        return handler(pretty=history_state.pretty, **history_kwargs)

    @staticmethod
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
            The exit code to abort with (defaults to 2, the standard for CLI
            usage errors).

        Raises
        ------
        typer.Exit
            Always raises to abort the CLI command.
        """
        typer.echo(f'Error: {message}', err=True)
        raise typer.Exit(exit_code)

    @staticmethod
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
            The file format value to normalize.
        label : str
            The label for the file format option, used in error messages.

        Returns
        -------
        FileFormat | None
            The normalized file format value.
        """
        normalized = ResourceTypeResolver.optional_choice(
            None if value is None else str(value),
            FILE_FORMATS,
            label=label,
        )
        return None if normalized is None else FileFormat.coerce(normalized)

    @staticmethod
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
            The CLI flag associated with the JSON value.

        Returns
        -------
        Any
            The parsed JSON value.

        Raises
        ------
        typer.BadParameter
            If the JSON value is invalid.
        """
        try:
            return JsonCodec.parse(value)
        except ValueError as exc:
            raise typer.BadParameter(f'Invalid JSON for {flag}: {exc}') from exc

    @staticmethod
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
            The CLI value to check.
        message : str
            The error message to emit if the value is missing.
        positional_name : str | None, optional
            The name of the positional argument, used in error messages.

        Returns
        -------
        str
            The validated CLI value.
        """
        if not value:
            CommandHelperPolicy.fail_usage(message)
        if positional_name is not None and value.startswith('--'):
            CommandHelperPolicy.fail_usage(
                f"Option '{value}' must follow the '{positional_name}' argument.",
            )
        return value

    @staticmethod
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
            The CLI state.
        role : DataConnectorContext
            The resource role for error messages ('source' or 'target').
        value : str | None
            The raw CLI value to resolve.
        connector_type : str | None, optional
            An explicit connector type to validate against known connectors.
            Defaults to ``None``, which means no validation or normalization
            will be performed.
        format_value : FileFormat | str | None, optional
            An explicit file format hint to validate and normalize to
            :class:`FileFormat`. Defaults to ``None``, which means no
            validation or normalization will be performed.
        positional : bool, optional
            Whether the value comes from a positional argument, which requires
            additional validation to reject option-like values. Defaults to
            ``False``.
        soft_inference : bool, optional
            Whether to allow soft inference of the connector type based on the
            value when an explicit *connector_type* is not provided. Defaults
            to ``False``, which means the type will be ``None`` when not
            explicitly provided.
        default_value : str, optional
            The default value to use when *value* is ``None``. Defaults to '-',
            which is a common CLI convention for STDIN/STDOUT.

        Returns
        -------
        _ResolvedResource
            The resolved resource.
        """
        resolved_value = default_value if value is None else value
        if positional:
            resolved_value = CommandHelperPolicy.require_value(
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
                explicit_type=ResourceTypeResolver.optional_choice(
                    None if connector_type is None else str(connector_type),
                    DATA_CONNECTORS,
                    label=f'{role}_type',
                ),
                soft_inference=soft_inference,
            ),
            format_hint=CommandHelperPolicy.normalize_file_format(
                format_value,
                label=f'{role}_format',
            ),
        )

    @staticmethod
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
        Return one CLI state plus one normalized resource for a command
        wrapper.

        Parameters
        ----------
        ctx : typer.Context
            Typer context used to initialize CLI state when *state* is not
            given.
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
        return resolved_state, CommandHelperPolicy.resolve_resource(
            resolved_state,
            role=role,
            value=value,
            connector_type=connector_type,
            format_value=format_value,
            positional=positional,
            soft_inference=soft_inference,
            default_value=default_value,
        )
