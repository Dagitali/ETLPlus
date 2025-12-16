"""
:mod:`etlplus.config.utils` module.

A module defining utility helpers for ETL pipeline configuration.

Notes
-----
- Inputs to parsers favor ``Mapping[str, Any]`` to remain permissive and
    avoid unnecessary copies; normalization returns concrete types.
- Substitution is shallow for strings and recursive for containers.
- Numeric coercion helpers are intentionally forgiving: invalid values
    become ``None`` rather than raising.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from ..types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'deep_substitute',
]


# SECTION: FUNCTIONS ======================================================== #


def deep_substitute(
    value: Any,
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, str] | None,
) -> Any:
    """
    Recursively substitute ``${VAR}`` tokens in nested structures.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : StrAnyMap | None
        Mapping of variable names to replacement values (lower precedence).
    env_map : Mapping[str, str] | None
        Mapping of environment variables overriding ``vars_map`` values (higher
        precedence).

    Returns
    -------
    Any
        New structure with substitutions applied where tokens were found.
    """
    substitutions = _prepare_substitutions(vars_map, env_map)

    def _apply(node: Any) -> Any:
        match node:
            case str():
                return _replace_tokens(node, substitutions)
            case Mapping():
                return {k: _apply(v) for k, v in node.items()}
            case list() | tuple() as seq:
                apply = [_apply(item) for item in seq]
                return apply if isinstance(seq, list) else tuple(apply)
            case set():
                return {_apply(item) for item in node}
            case frozenset():
                return frozenset(_apply(item) for item in node)
            case _:
                return node

    return _apply(value)


# SECTION: INTERNAL FUNCTIONS ============================================== #


def _prepare_substitutions(
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, Any] | None,
) -> tuple[tuple[str, Any], ...]:
    if not vars_map and not env_map:
        return ()
    merged: dict[str, Any] = {}
    if vars_map:
        merged.update(vars_map)
    if env_map:
        merged.update(env_map)
    return tuple(merged.items())


def _replace_tokens(
    text: str,
    substitutions: Iterable[tuple[str, Any]],
) -> str:
    if not substitutions:
        return text
    out = text
    for name, replacement in substitutions:
        token = f"${{{name}}}"
        if token in out:
            out = out.replace(token, str(replacement))
    return out
