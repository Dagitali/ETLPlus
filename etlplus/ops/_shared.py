"""
:mod:`etlplus.ops._shared` module.

Internal helpers shared across ETL operation modules.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import Final
from typing import cast

from ..file.base import ReadOptions
from ..file.base import WriteOptions
from ..utils._types import JSONDict

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'coerce_read_options',
    'coerce_write_options',
    'index_named_items',
    'merge_mapping_options',
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


def _coerce_optional_text(
    value: object,
) -> str | None:
    """Return ``None`` unchanged and stringify non-string values."""
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _coerce_required_text(
    value: object,
    *,
    default: str,
) -> str:
    """Return a required text value with a string default."""
    if value is None:
        return default
    return value if isinstance(value, str) else str(value)


def _coerce_file_options[OptionsT](
    options: OptionsT | Mapping[str, Any] | None,
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
        normalized[key] = _coerce_optional_text(normalized.get(key))

    return factory(
        **normalized,
        extras=cast(JSONDict, extras),
    )


# SECTION: FUNCTIONS ======================================================== #


def coerce_read_options(
    options: ReadOptions | Mapping[str, Any] | None,
) -> ReadOptions | None:
    """
    Normalize mapping-based read options into :class:`ReadOptions`.

    Parameters
    ----------
    options : ReadOptions | Mapping[str, Any] | None
        Read options to normalize. If already a :class:`ReadOptions` instance
        or ``None``, this is returned unchanged. Otherwise, if a mapping, the
        following keys are extracted and normalized with the specified
        defaults:
        - ``dataset``: The dataset name for structured files (default:
            ``None``).
        - ``encoding``: The text encoding to use (default: ``'utf-8'``).
        - ``inner_name``: The inner name for nested structures (default:
            ``None``).
        - ``sheet``: The sheet name or index for spreadsheet files (default:
            ``None``).
        - ``table``: The table name for database or structured files (default:
            ``None``).

    Returns
    -------
    ReadOptions | None
        A normalized :class:`ReadOptions` instance or ``None`` if the input was
        ``None``.
    """
    return _coerce_file_options(
        options,
        option_type=ReadOptions,
        factory=ReadOptions,
        defaults=_READ_OPTION_DEFAULTS,
    )


def coerce_write_options(
    options: WriteOptions | Mapping[str, Any] | None,
) -> WriteOptions | None:
    """
    Normalize mapping-based write options into :class:`WriteOptions`.

    Parameters
    ----------
    options : WriteOptions | Mapping[str, Any] | None
        Write options to normalize. If already a :class:`WriteOptions` instance
        or ``None``, this is returned unchanged. Otherwise, if a mapping, the
        following keys are extracted and normalized with the specified
        defaults:
        - ``dataset``: The dataset name for structured files (default:
            ``None``).
        - ``encoding``: The text encoding to use (default: ``'utf-8'``).
        - ``inner_name``: The inner name for nested structures (default:
            ``None``).
        - ``sheet``: The sheet name or index for spreadsheet files (default:
            ``None``).
        - ``table``: The table name for database or structured files (default:
            ``None``).
        - ``root_tag``: The root tag name for XML files (default: ``'root'``).

    Returns
    -------
    WriteOptions | None
        A normalized :class:`WriteOptions` instance or ``None`` if the input
        was ``None``.
    """
    return _coerce_file_options(
        options,
        option_type=WriteOptions,
        factory=WriteOptions,
        defaults=_WRITE_OPTION_DEFAULTS,
    )


def index_named_items[ItemT](
    items: Iterable[ItemT],
    *,
    item_label: str,
) -> dict[str, ItemT]:
    """
    Index named items and reject duplicates with a descriptive error.

    Items without a non-empty string ``name`` attribute are ignored.

    Parameters
    ----------
    items : Iterable[ItemT]
        An iterable of items to index. Each item must have a ``name`` attribute
        that is a non-empty string to be included in the index.
    item_label : str
        A label for the type of items being indexed, used in error messages.

    Returns
    -------
    dict[str, ItemT]
        A dictionary mapping item names to their corresponding items. Only
        items with a valid non-empty string ``name`` attribute are included. If
        duplicate names are found, a ValueError is raised indicating the
        duplicate name and item type.

    Raises
    ------
    ValueError
        If duplicate names are found.
    """
    indexed: dict[str, ItemT] = {}
    for item in items:
        if not isinstance(name := getattr(item, 'name', None), str) or not name:
            continue
        if name in indexed:
            raise ValueError(f'Duplicate {item_label} name: {name}')
        indexed[name] = item
    return indexed


def merge_mapping_options(
    *option_sets: object,
    excluded_keys: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """
    Merge mapping-like option sets with later mappings taking precedence.

    Parameters
    ----------
    *option_sets : object
        Option sets to merge. Each option set can be a mapping or any other
        object. Only mappings are considered for merging.
    excluded_keys : frozenset[str], optional
        Keys to exclude from the merged result (default: empty frozenset).

    Returns
    -------
    dict[str, Any]
        A dictionary containing the merged options, with later mappings
        taking precedence and excluded keys removed.
    """
    merged: dict[str, Any] = {}
    for option_set in option_sets:
        if isinstance(option_set, Mapping):
            merged.update(option_set)
    for key in excluded_keys:
        merged.pop(key, None)
    return merged
