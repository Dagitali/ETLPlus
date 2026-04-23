"""
:mod:`etlplus.ops._options` module.

Helpers for normalizing file read/write option mappings.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from typing import Final
from typing import cast

from ..file.base import ReadOptions
from ..file.base import WriteOptions
from ..utils import ValueParser
from ..utils._types import JSONDict
from ._types import FileOptionsArg

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'coerce_read_options',
    'coerce_write_options',
]


# SECTION: CONSTANTS ======================================================== #


_OPTIONAL_TEXT_FIELDS: Final[frozenset[str]] = frozenset(
    {'table', 'dataset', 'inner_name'},
)

_READ_OPTION_DEFAULTS: Final[dict[str, object]] = {
    'encoding': 'utf-8',
    'sheet': None,
    'table': None,
    'dataset': None,
    'inner_name': None,
}

_WRITE_OPTION_DEFAULTS: Final[dict[str, object]] = {
    **_READ_OPTION_DEFAULTS,
    'root_tag': 'root',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_required_text(
    value: object,
    *,
    default: str,
) -> str:
    """Return a required text value with a string default."""
    return default if (text := ValueParser.optional_str(value)) is None else text


def _coerce_file_options[OptionsT](
    options: FileOptionsArg[OptionsT],
    *,
    option_type: type[OptionsT],
    factory: Callable[..., OptionsT],
    defaults: Mapping[str, object],
) -> OptionsT | None:
    """Normalize mapping-based file options into a concrete options object."""
    if options is None or isinstance(options, option_type):
        return options
    if not isinstance(options, Mapping):
        raise TypeError(
            'options must be a mapping or a concrete file options object',
        )

    extras = {key: value for key, value in options.items()}
    normalized = {key: extras.pop(key, default) for key, default in defaults.items()}

    normalized['encoding'] = _coerce_required_text(
        normalized.get('encoding'),
        default='utf-8',
    )
    if 'root_tag' in normalized:
        normalized['root_tag'] = _coerce_required_text(
            normalized.get('root_tag'),
            default='root',
        )
    for key in _OPTIONAL_TEXT_FIELDS:
        normalized[key] = ValueParser.optional_str(normalized.get(key))

    return factory(
        **normalized,
        extras=cast(JSONDict, extras),
    )


# SECTION: FUNCTIONS ======================================================== #


def coerce_read_options(
    options: FileOptionsArg[ReadOptions],
) -> ReadOptions | None:
    """
    Normalize mapping-based read options into :class:`ReadOptions`.

    Parameters
    ----------
    options : FileOptionsArg[ReadOptions]
        Read options to normalize. Can be a :class:`ReadOptions` instance, a
        mapping of option values, or ``None``.

    Returns
    -------
    ReadOptions | None
        Normalized read options. Returns ``None`` if the input is ``None``.
    """
    return _coerce_file_options(
        options,
        option_type=ReadOptions,
        factory=ReadOptions,
        defaults=_READ_OPTION_DEFAULTS,
    )


def coerce_write_options(
    options: FileOptionsArg[WriteOptions],
) -> WriteOptions | None:
    """
    Normalize mapping-based write options into :class:`WriteOptions`.

    Parameters
    ----------
    options : FileOptionsArg[WriteOptions]
        Write options to normalize. Can be a :class:`WriteOptions` instance, a
        mapping of option values, or ``None``.

    Returns
    -------
    WriteOptions | None
        Normalized write options. Returns ``None`` if the input is ``None``.
    """
    return _coerce_file_options(
        options,
        option_type=WriteOptions,
        factory=WriteOptions,
        defaults=_WRITE_OPTION_DEFAULTS,
    )
