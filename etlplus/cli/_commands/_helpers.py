"""
:mod:`etlplus.cli._commands._helpers` module.

Shared helper functions for CLI command modules.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import Literal
from typing import NoReturn
from typing import Self

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


@dataclass(frozen=True, slots=True)
class CommandHelperPolicy:
    """Own shared command dispatch, validation, and resource normalization."""

    # -- Instance Attributes -- #

    state: CliState

    # -- Class Methods -- #

    @classmethod
    def from_context(
        cls,
        ctx: typer.Context,
    ) -> Self:
        """
        Return one command helper policy using CLI state from *ctx*.

        Parameters
        ----------
        ctx : typer.Context
            Typer context used to resolve shared CLI state.

        Returns
        -------
        Self
            A command helper policy bound to the current CLI state.
        """
        return cls(ensure_state(ctx))

    # -- Instance Methods -- #

    def call_handler(
        self,
        handler: Callable[..., int],
        /,
        *,
        state_fields: tuple[_StateField, ...] = ('pretty',),
        **kwargs: Any,
    ) -> int:
        """
        Invoke one handler with selected state-derived keyword arguments.

        Parameters
        ----------
        handler : Callable[..., int]
            The handler function to invoke.
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
        return handler(
            **kwargs,
            **{field: getattr(self.state, field) for field in state_fields},
        )

    def call_history_command(
        self,
        handler: Callable[..., int],
        **kwargs: Any,
    ) -> int:
        """
        Invoke one history-style command using this policy's CLI state.

        Parameters
        ----------
        handler : Callable[..., int]
            The history-oriented handler function to invoke.
        **kwargs : Any
            Additional keyword arguments to pass to *handler*.

        Returns
        -------
        int
            The exit code returned by *handler*.
        """
        return self.call_handler(
            handler,
            **kwargs,
        )

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

    def resolve_resource(
        self,
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
        explicit_type = (
            None
            if connector_type is None
            else ResourceTypeResolver.validate(
                str(connector_type),
                DATA_CONNECTORS,
                label=f'{role}_type',
            )
        )
        normalized_format = (
            None
            if format_value is None
            else ResourceTypeResolver.validate(
                str(format_value),
                FILE_FORMATS,
                label=f'{role}_format',
            )
        )
        return _ResolvedResource(
            value=resolved_value,
            resource_type=_resolve_logged_resource_type(
                self.state,
                role=role,
                value=resolved_value,
                explicit_type=explicit_type,
                soft_inference=soft_inference,
            ),
            format_hint=(
                None
                if normalized_format is None
                else FileFormat.coerce(normalized_format)
            ),
        )
