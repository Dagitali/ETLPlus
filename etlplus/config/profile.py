"""
etlplus.config.profile
======================

A module defining configuration types for ETL job pipeline profiles.
"""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ProfileConfig:
    """
    Configuration for pipeline profiles.
    """

    # -- Attributes -- #

    default_target: str | None = None
    env: dict[str, str] = field(default_factory=dict)
