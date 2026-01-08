"""
:mod:`etlplus.database` package.

This package defines database-related utilities for ETLPlus, including:
- DDL rendering and schema management.
"""

from __future__ import annotations

from .ddl import load_table_spec
from .ddl import render_table_sql
from .ddl import render_tables
from .ddl import render_tables_to_string

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'load_table_spec',
    'render_table_sql',
    'render_tables',
    'render_tables_to_string',
]
