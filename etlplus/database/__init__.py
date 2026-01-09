"""
:mod:`etlplus.database` package.

Database utilities for:
- DDL rendering and schema management.
- Schema parsing from configuration files.
- Dynamic ORM generation.
- Database engine/session management.
"""

from __future__ import annotations

from .ddl import load_table_spec
from .ddl import render_table_sql
from .ddl import render_tables
from .ddl import render_tables_to_string
from .engine import engine
from .engine import load_database_url_from_config
from .engine import make_engine
from .engine import session
from .orm import Base
from .orm import build_models
from .orm import load_and_build_models
from .schema import load_table_specs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'build_models',
    'load_and_build_models',
    'load_database_url_from_config',
    'load_table_spec',
    'load_table_specs',
    'make_engine',
    'render_table_sql',
    'render_tables',
    'render_tables_to_string',
    'Base',
    # Singletons
    'engine',
    'session',
]
