"""
:mod:`etlplus.config.profile` module.

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

from ..types import StrAnyMap
from ..utils import cast_str_dict

# SECTION: EXPORTS ========================================================== #


__all__ = ['ProfileConfig']


# SECTION: CLASSES ========================================================== #


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
        obj: StrAnyMap | None,
    ) -> Self:
        """Parse a mapping into a ``ProfileConfig`` instance.

        Parameters
        ----------
        obj : StrAnyMap | None
            Mapping with optional profile fields, or ``None``.

        Returns
        -------
        Self
            Parsed profile configuration; non-mapping input yields a default
            instance. All ``env`` values are coerced to strings.
        """
        if not isinstance(obj, Mapping):
            return cls()

        # Coerce all env values to strings using shared helper.
        env = cast_str_dict(obj.get('env'))

        return cls(
            default_target=obj.get('default_target'),
            env=env,
        )
