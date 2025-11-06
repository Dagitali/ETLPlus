"""
etlplus.config.profile
======================

A module defining configuration types for ETL job pipeline profiles.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self

from .utils import cast_str_dict


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ProfileConfig:
    """
    Configuration for pipeline profiles.
    """

    # -- Attributes -- #

    default_target: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: Mapping[str, Any] | None,
    ) -> Self:
        """
        Create a ProfileConfig from a mapping.

        Coerces all env values to str and tolerates missing keys.

        Parameters
        ----------
        obj : Mapping[str, Any] | None
            The mapping to create the ProfileConfig from.
        """

        if not isinstance(obj, Mapping):
            return cls()

        env = cast_str_dict(obj.get('env') if isinstance(obj, Mapping) else {})

        return cls(
            default_target=obj.get('default_target'),
            env=env,
        )
