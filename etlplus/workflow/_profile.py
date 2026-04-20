"""
:mod:`etlplus.workflow._profile` module.

Profile model for pipeline-level defaults and environment.

Notes
-----
- Accepts ``Mapping[str, Any]`` and normalizes to concrete types.
- Environment values are coerced to strings.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Self

from ..utils import MappingParser
from ..utils import ValueParser

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ProfileConfig',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ProfileConfig:
    """
    Configuration for pipeline profiles.

    Attributes
    ----------
    default_target : str | None
        Default target name for jobs that omit an explicit target.
    env : dict[str, str]
        Environment variables available for substitution.
    """

    # -- Attributes -- #

    default_target: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: object,
    ) -> Self:
        """
        Parse a mapping into a :class:`ProfileConfig` instance.

        Parameters
        ----------
        obj : object
            Mapping with optional profile fields, or ``None``.

        Returns
        -------
        Self
            Parsed profile configuration; non-mapping input yields a default
            instance. All :attr:`env` values are coerced to strings.
        """
        if not isinstance(obj, Mapping):
            return cls()

        # Coerce all env values to strings using shared helper.
        env = MappingParser.to_str_dict(MappingParser.optional(obj.get('env')))

        return cls(
            default_target=ValueParser.optional_str(
                obj.get('default_target'),
            ),
            env=env,
        )
