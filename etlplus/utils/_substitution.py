"""
:mod:`etlplus.utils._substitution` module.

Substitution utility helpers.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from re import Pattern
from typing import Any
from typing import Final

from ._mapping import MappingParser
from ._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SubstitutionResolver',
    'TokenReferenceCollector',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_TOKEN_PATTERN: Final[Pattern[str]] = re.compile(r'\$\{([^}]+)\}')


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _replace_tokens(
    text: str,
    substitutions: Iterable[tuple[str, Any]],
) -> str:
    """
    Replace ``${VAR}`` tokens in *text* using *substitutions*.

    Parameters
    ----------
    text : str
        Input string that may contain ``${VAR}`` tokens.
    substitutions : Iterable[tuple[str, Any]]
        Sequence of ``(name, value)`` pairs used for token replacement.

    Returns
    -------
    str
        Updated text with replacements applied.
    """
    out = text
    for name, replacement in substitutions:
        token = f'${{{name}}}'
        if token in out:
            out = out.replace(token, str(replacement))
    return out


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class SubstitutionResolver:
    """
    Resolve token substitutions across nested Python containers.

    Attributes
    ----------
    vars_map : StrAnyMap | None
        Mapping of variable names to replacement values (lower precedence).
    env_map : Mapping[str, object] | None
        Mapping of environment variables overriding *vars_map* values (higher
        precedence).
    """

    vars_map: StrAnyMap | None = None
    env_map: Mapping[str, object] | None = None

    # -- Getters -- #

    @property
    def substitutions(self) -> tuple[tuple[str, Any], ...]:
        """Return merged substitutions in replacement order."""
        if not self.vars_map and not self.env_map:
            return ()
        return tuple(
            MappingParser.merge_to_dict(self.vars_map, self.env_map).items(),
        )

    # -- Instance Methods -- #

    def deep(
        self,
        value: Any,
    ) -> Any:
        """
        Recursively substitute ``${VAR}`` tokens in nested structures.

        Only strings are substituted; other types are returned as-is.

        Parameters
        ----------
        value : Any
            The value to perform substitutions on.

        Returns
        -------
        Any
            New structure with substitutions applied where tokens were found.
        """
        substitutions = self.substitutions
        if not substitutions:
            return value

        def _apply(node: Any) -> Any:
            match node:
                case str():
                    return _replace_tokens(node, substitutions)
                case Mapping():
                    return {k: _apply(v) for k, v in node.items()}
                case list() | tuple() as seq:
                    resolved = [_apply(item) for item in seq]
                    return resolved if isinstance(seq, list) else tuple(resolved)
                case set():
                    return {_apply(item) for item in node}
                case frozenset():
                    return frozenset(_apply(item) for item in node)
                case _:
                    return node

        return _apply(value)


@dataclass(slots=True)
class TokenReferenceCollector:
    """
    Collect unresolved text tokens and their stable paths in nested values.

    Attributes
    ----------
    pattern : Pattern[str]
        Regex pattern whose first capture group is treated as the token name.
    """

    # -- Instance Attributes -- #

    pattern: Pattern[str] = _DEFAULT_TOKEN_PATTERN
    paths_by_name: dict[str, set[str]] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def collect_names(
        cls,
        value: Any,
        *,
        pattern: Pattern[str] | None = None,
    ) -> set[str]:
        """Return one set of token names discovered in *value*."""
        collector = cls(pattern=pattern or _DEFAULT_TOKEN_PATTERN)
        collector.walk(value)
        return set(collector.paths_by_name)

    @classmethod
    def collect_rows(
        cls,
        value: Any,
        *,
        pattern: Pattern[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return one stable list of token reference rows."""
        collector = cls(pattern=pattern or _DEFAULT_TOKEN_PATTERN)
        collector.walk(value)
        return [
            {'name': name, 'paths': sorted(paths)}
            for name, paths in sorted(collector.paths_by_name.items())
        ]

    # -- Instance Methods -- #

    def walk(
        self,
        node: Any,
        path: str = '',
    ) -> None:
        """Record token names and paths found in *node*."""
        match node:
            case str():
                for match in self.pattern.findall(node):
                    self.paths_by_name.setdefault(match, set()).add(path or '<root>')
            case Mapping():
                for key, inner in node.items():
                    key_text = str(key)
                    next_path = f'{path}.{key_text}' if path else key_text
                    self.walk(inner, next_path)
            case list() | tuple() as seq:
                for index, inner in enumerate(seq):
                    next_path = f'{path}[{index}]' if path else f'[{index}]'
                    self.walk(inner, next_path)
            case set() | frozenset():
                for index, inner in enumerate(sorted(node, key=repr)):
                    next_path = f'{path}[{index}]' if path else f'[{index}]'
                    self.walk(inner, next_path)
            case _:
                return
