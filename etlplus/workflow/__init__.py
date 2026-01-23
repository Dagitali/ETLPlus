"""
:mod:`etlplus.workflow` package.

Job workflow helpers.
"""

from __future__ import annotations

from .connector import Connector
from .connector import ConnectorApi
from .connector import ConnectorDb
from .connector import ConnectorFile
from .connector import parse_connector
from .dag import topological_sort_jobs
from .jobs import ExtractRef
from .jobs import JobConfig
from .jobs import LoadRef
from .jobs import TransformRef
from .jobs import ValidationRef
from .pipeline import PipelineConfig
from .pipeline import load_pipeline_config

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ConnectorApi',
    'ConnectorDb',
    'ConnectorFile',
    'ExtractRef',
    'JobConfig',
    'LoadRef',
    'PipelineConfig',
    'TransformRef',
    'ValidationRef',
    # Functions
    'load_pipeline_config',
    'parse_connector',
    'topological_sort_jobs',
    # Type Aliases
    'Connector',
]
