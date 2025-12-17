"""
:mod:`etlplus.config` package.

Configuration models and helpers for ETLPlus.

This package defines models for data sources/targets ("connectors"), APIs,
pagination/rate limits, pipeline orchestration, and related utilities. The
parsers are permissive (accepting ``Mapping[str, Any]``) and normalize to
concrete types without raising on unknown/optional fields.

Notes
-----
- The models use ``@dataclass(slots=True)`` and avoid mutating inputs.
- TypedDicts are editor/type-checking hints and are not enforced at runtime.
"""

from __future__ import annotations

from .connector import Connector
from .connector import ConnectorApi
from .connector import ConnectorDb
from .connector import ConnectorFile
from .connector import parse_connector
from .jobs import ExtractRef
from .jobs import JobConfig
from .jobs import LoadRef
from .jobs import TransformRef
from .jobs import ValidationRef
from .pipeline import PipelineConfig
from .pipeline import load_pipeline_config
from .profile import ProfileConfig
from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Connectors
    'Connector',
    'ConnectorType',
    'ConnectorApi',
    'ConnectorDb',
    'ConnectorFile',
    'parse_connector',
    # Jobs / Refs
    'ExtractRef',
    'JobConfig',
    'LoadRef',
    'TransformRef',
    'ValidationRef',
    # Pipeline
    'PipelineConfig',
    'load_pipeline_config',
    # Profile
    'ProfileConfig',
]
