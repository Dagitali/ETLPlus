"""
etlplus.config.__init__
====================

The top-level module defining ``:mod:etlplus.config``, a package of classes for
defining ETL pipeline configurations including data sources, data targets,
jobs, and profiles as well as helper functions to load and parse them.

The configuration classes represent a tolerant schema for pipeline YAML files
like `in/pipeline.yml`. They aim to cover common shapes while allowing
provider-specific options to pass through as dictionaries.

"""
from __future__ import annotations

from .api import ApiConfig
from .api import ApiProfileConfig
from .api import EndpointConfig
from .connector import ConnectorApi
from .connector import ConnectorDb
from .connector import ConnectorFile
from .connector import parse_connector
from .jobs import ExtractRef
from .jobs import JobConfig
from .jobs import LoadRef
from .jobs import TransformRef
from .jobs import ValidationRef
from .pagination import PaginationConfig
from .pipeline import load_pipeline_config
from .pipeline import PipelineConfig
from .profile import ProfileConfig
from .rate_limit import RateLimitConfig
from .types import Connector
from .types import ConnectorType

__all__ = [
    # API
    'ApiConfig',
    'ApiProfileConfig',
    'EndpointConfig',
    'PaginationConfig',
    'RateLimitConfig',

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
