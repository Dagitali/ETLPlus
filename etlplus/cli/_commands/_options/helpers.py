"""
:mod:`etlplus.cli._commands._options.helpers` module.

Shared Typer option aliases and keyword factories for CLI command modules.
"""

from __future__ import annotations

from typing import Annotated
from typing import Any
from typing import Literal

import typer

from .._types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'typer_connector_option_alias',
    'typer_flag_option_alias',
    'typer_flag_option_kwargs',
    'typer_format_option_alias',
    'typer_option_alias',
    'typer_resource_argument_alias',
    'typer_resource_argument_kwargs',
    'typer_timestamp_option_alias',
    'typer_value_option_alias',
    'typer_value_option_kwargs',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _typer_argument_alias(
    value_type: Any,
    *param_decls: object,
    **kwargs: Any,
) -> Any:
    """Return one ``Annotated`` Typer argument alias."""
    return Annotated[
        value_type,
        typer.Argument(*param_decls, **kwargs),  # type: ignore[call-overload]
    ]


def _typer_connector_option_kwargs(
    *,
    context: DataConnectorContext,
    rich_help_panel: str = 'I/O overrides',
) -> dict[str, object]:
    """Return common Typer option kwargs for source/target connector types."""
    return {
        'metavar': 'CONNECTOR',
        'show_default': False,
        'rich_help_panel': rich_help_panel,
        'help': f'Override the inferred {context} type (api, database, file).',
    }


def _typer_format_option_kwargs(
    *,
    context: DataConnectorContext,
    rich_help_panel: str = 'Format overrides',
) -> dict[str, object]:
    """Return common Typer option kwargs for format overrides."""
    return {
        'metavar': 'FORMAT',
        'show_default': False,
        'rich_help_panel': rich_help_panel,
        'help': (
            f'Payload format when the {context} is STDIN/inline or a '
            'non-file connector. File connectors infer from extensions.'
        ),
    }


def _typer_timestamp_option_kwargs(
    *,
    bound: Literal['since', 'until'],
) -> dict[str, object]:
    """Return common Typer option kwargs for ISO-8601 history bounds."""
    direction = 'after' if bound == 'since' else 'before'
    return {
        'metavar': 'ISO8601',
        'show_default': False,
        'help': (f'Emit only records at or {direction} the given ISO-8601 timestamp.'),
    }


# SECTION: FUNCTIONS ======================================================== #


def typer_option_alias(
    value_type: Any,
    *param_decls: object,
    **kwargs: Any,
) -> Any:
    """
    Return one ``Annotated`` Typer option alias.

    Parameters
    ----------
    value_type : Any
        The type of the option value.
    *param_decls : object
        The Typer option parameter declarations, e.g. ``'--verbose'``.
    **kwargs : Any
        Additional keyword arguments to pass to the Typer option.

    Returns
    -------
    Any
        The annotated Typer option alias.
    """
    return Annotated[
        value_type,
        typer.Option(*param_decls, **kwargs),  # type: ignore[call-overload]
    ]


def typer_connector_option_alias(
    value_type: Any,
    *param_decls: object,
    context: DataConnectorContext,
    rich_help_panel: str = 'I/O overrides',
) -> Any:
    """
    Return one connector-type Typer option alias.

    Parameters
    ----------
    value_type : Any
        The type of the option value.
    *param_decls : object
        The Typer option parameter declarations, e.g. ``'--source-type'``.
    context : DataConnectorContext
        Either ``'source'`` or ``'target'`` to tailor help text.
    rich_help_panel : str, optional
        The rich help panel name. Default is ``'I/O overrides'``.

    Returns
    -------
    Any
        The annotated Typer option alias.
    """
    return typer_option_alias(
        value_type,
        *param_decls,
        **_typer_connector_option_kwargs(
            context=context,
            rich_help_panel=rich_help_panel,
        ),
    )


def typer_flag_option_kwargs(
    help_text: str,
    *,
    is_eager: bool = False,
    show_default: bool | None = None,
) -> dict[str, object]:
    """
    Return common Typer option kwargs for simple boolean flags.

    Parameters
    ----------
    help_text : str
        The help text for the option.
    is_eager : bool, optional
        Whether the option is eager. Default is ``False``.
    show_default : bool | None, optional
        Whether to show the default value. Default is ``None``.

    Returns
    -------
    dict[str, object]
        The common Typer option kwargs for simple boolean flags.
    """
    kwargs: dict[str, object] = {'help': help_text}
    if is_eager:
        kwargs['is_eager'] = True
    if show_default is not None:
        kwargs['show_default'] = show_default
    return kwargs


def typer_flag_option_alias(
    *param_decls: object,
    help_text: str,
    is_eager: bool = False,
    show_default: bool | None = None,
) -> Any:
    """
    Return one boolean-flag Typer option alias.

    Parameters
    ----------
    *param_decls : object
        The Typer option parameter declarations, e.g. ``'--verbose'``.
    help_text : str
        The help text for the option.
    is_eager : bool, optional
        Whether the option is eager. Default is ``False``.
    show_default : bool | None, optional
        Whether to show the default value. Default is ``None``.

    Returns
    -------
    Any
        The annotated Typer option alias.
    """
    return typer_option_alias(
        bool,
        *param_decls,
        **typer_flag_option_kwargs(
            help_text,
            is_eager=is_eager,
            show_default=show_default,
        ),
    )


def typer_format_option_alias(
    value_type: Any,
    *param_decls: object,
    context: DataConnectorContext,
    rich_help_panel: str = 'Format overrides',
) -> Any:
    """
    Return one format-override Typer option alias.

    Parameters
    ----------
    value_type : Any
        The type of the option value.
    *param_decls : object
        The Typer option parameter declarations, e.g. ``'--source-format'``.
    context : DataConnectorContext
        Either ``'source'`` or ``'target'`` to tailor help text.
    rich_help_panel : str, optional
        The rich help panel name. Default is ``'Format overrides'``.

    Returns
    -------
    Any
        The annotated Typer option alias.
    """
    return typer_option_alias(
        value_type,
        *param_decls,
        **_typer_format_option_kwargs(
            context=context,
            rich_help_panel=rich_help_panel,
        ),
    )


def typer_resource_argument_kwargs(
    *,
    context: DataConnectorContext,
) -> dict[str, object]:
    """
    Return common Typer argument kwargs for source/target resources.

    Parameters
    ----------
    context : DataConnectorContext
        Either ``'source'`` or ``'target'`` to tailor help text.

    Returns
    -------
    dict[str, object]
        The common Typer argument kwargs for the given context.
    """
    if context == 'source':
        description = 'JSON payload, file path, URI/URL, or - for STDIN'
        verb = 'Extract data from'
    else:
        description = 'file path, URI/URL, or - for STDOUT'
        verb = 'Load data into'
    return {
        'metavar': context.upper(),
        'help': (
            f'{verb} {context.upper()} ({description}). Use '
            f'--{context}-format to override '
            'the inferred data format and '
            f'--{context}-type to override the inferred data connector.'
        ),
    }


def typer_resource_argument_alias(
    value_type: Any,
    *param_decls: object,
    context: DataConnectorContext,
) -> Any:
    """
    Return one source/target resource Typer argument alias.

    Parameters
    ----------
    value_type : Any
        The type of the argument value.
    *param_decls : object
        The Typer argument parameter declarations, e.g. ``'SOURCE'``.
    context : DataConnectorContext
        Either ``'source'`` or ``'target'`` to tailor help text.

    Returns
    -------
    Any
        The annotated Typer argument alias.
    """
    return _typer_argument_alias(
        value_type,
        *param_decls,
        **typer_resource_argument_kwargs(context=context),
    )


def typer_timestamp_option_alias(
    value_type: Any,
    *param_decls: object,
    bound: Literal['since', 'until'],
) -> Any:
    """Return one history-bound Typer option alias."""
    return typer_option_alias(
        value_type,
        *param_decls,
        **_typer_timestamp_option_kwargs(bound=bound),
    )


def typer_value_option_kwargs(
    help_text: str,
    *,
    metavar: str | None = None,
    show_default: bool | None = False,
) -> dict[str, object]:
    """
    Return common Typer option kwargs for scalar string-like inputs.

    Parameters
    ----------
    help_text : str
        The help text for the option.
    metavar : str | None, optional
        The metavar for the option. Default is ``None``.
    show_default : bool | None, optional
        Whether to show the default value. Default is ``False``.

    Returns
    -------
    dict[str, object]
        The common Typer option kwargs for scalar string-like inputs.
    """
    kwargs: dict[str, object] = {'help': help_text}
    if metavar is not None:
        kwargs['metavar'] = metavar
    if show_default is not None:
        kwargs['show_default'] = show_default
    return kwargs


def typer_value_option_alias(
    value_type: Any,
    *param_decls: object,
    help_text: str,
    metavar: str | None = None,
    show_default: bool | None = False,
    **kwargs: Any,
) -> Any:
    """
    Return one scalar Typer option alias.

    Parameters
    ----------
    value_type : Any
        The type of the option value.
    *param_decls : object
        The Typer option parameter declarations, e.g. ``'--verbose'``.
    help_text : str
        The help text for the option.
    metavar : str | None, optional
        The metavar for the option. Default is ``None``.
    show_default : bool | None, optional
        Whether to show the default value. Default is ``False``.
    **kwargs : Any
        Additional keyword arguments to pass to the Typer option.

    Returns
    -------
    Any
        The annotated Typer option alias.
    """
    return typer_option_alias(
        value_type,
        *param_decls,
        **typer_value_option_kwargs(
            help_text,
            metavar=metavar,
            show_default=show_default,
        ),
        **kwargs,
    )
