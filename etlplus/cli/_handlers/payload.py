"""
:mod:`etlplus.cli._handlers.payload` module.

Payload-resolution helpers shared by CLI handler implementations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ...utils._types import TemplateKey
from .. import _io

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'resolve_mapping_payload',
    'resolve_payload',
    'resolve_render_template',
]


# SECTION: FUNCTIONS ======================================================== #


def resolve_payload(
    payload: object,
    *,
    format_hint: str | None,
    format_explicit: bool,
    hydrate_files: bool = True,
) -> object:
    """
    Resolve one CLI payload through the shared CLI payload loader.

    Parameters
    ----------
    payload : object
        The raw payload to resolve, typically from a CLI argument.
    format_hint : str | None
        An optional hint for the payload format, such as 'json' or 'yaml'.
    format_explicit : bool
        Whether the payload format was explicitly specified by the user.
    hydrate_files : bool, optional
        Whether to hydrate file references in the resolved payload. Defaults to
        ``True``.

    Returns
    -------
    object
        The resolved payload.

    """
    resolve_kwargs: dict[str, Any] = {
        'format_hint': format_hint,
        'format_explicit': format_explicit,
    }
    if not hydrate_files:
        resolve_kwargs['hydrate_files'] = False
    return _io.resolve_cli_payload(payload, **resolve_kwargs)


def resolve_mapping_payload(
    payload: object,
    *,
    format_explicit: bool,
    error_message: str,
) -> dict[str, Any]:
    """
    Resolve one CLI payload and require a mapping result.

    Parameters
    ----------
    payload : object
        The raw payload to resolve, typically from a CLI argument.
    format_explicit : bool
        Whether the payload format was explicitly specified by the user.
    error_message : str
        The error message to raise if the resolved payload is not a mapping.

    Returns
    -------
    dict[str, Any]
        The resolved mapping payload.

    Raises
    ------
    ValueError
        If the resolved payload is not a mapping.
    """
    resolved_payload = resolve_payload(
        payload,
        format_hint=None,
        format_explicit=format_explicit,
    )
    if not isinstance(resolved_payload, dict):
        raise ValueError(error_message)
    return resolved_payload


def resolve_render_template(
    template: TemplateKey | None,
    template_path: str | None,
) -> tuple[TemplateKey | None, str | None]:
    """
    Resolve a key or path for a render template.

    Parameters
    ----------
    template : TemplateKey | None
        The template key to resolve. If None, defaults to 'ddl'.
    template_path : str | None
        The path to the template file. If provided, takes precedence over the
        template key.

    Returns
    -------
    tuple[TemplateKey | None, str | None]
        A tuple containing the resolved template key and template path.
    """
    template_key = template or 'ddl'
    if template_path is not None:
        return template_key, template_path

    candidate_path = Path(cast(str, template_key))
    if candidate_path.exists():
        return None, str(candidate_path)
    return template_key, None
