"""
:mod:`etlplus.database` package.

Database utilities for:
- DDL rendering and schema management.
- Schema parsing from configuration files.
- Dynamic ORM generation.
- Database engine/session management.
"""

from __future__ import annotations

from ._ddl import load_table_spec
from ._ddl import render_table_sql
from ._ddl import render_tables
from ._ddl import render_tables_to_string
from ._engine import engine
from ._engine import load_database_url_from_config
from ._engine import make_engine
from ._engine import session
from ._enums import DatabaseDialect
from ._enums import ReferentialAction
from ._enums import SqlTypeAffinity
from ._orm import Base
from ._orm import build_models
from ._orm import load_and_build_models
from ._orm import resolve_type
from ._schema import ColumnSpec
from ._schema import ForeignKeySpec
from ._schema import IdentitySpec
from ._schema import IndexSpec
from ._schema import PrimaryKeySpec
from ._schema import TableSpec
from ._schema import UniqueConstraintSpec
from ._schema import load_table_specs

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Base',
    'ColumnSpec',
    'DatabaseDialect',
    'ForeignKeySpec',
    'IdentitySpec',
    'IndexSpec',
    'PrimaryKeySpec',
    'ReferentialAction',
    'SqlTypeAffinity',
    'TableSpec',
    'UniqueConstraintSpec',
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
    'resolve_type',
    # Singletons
    'engine',
    'session',
]
