"""
:mod:`etlplus.database.engine` module.

Lightweight engine/session factory with optional config-driven URL loading.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Final

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from ..file import File
from ..types import StrAnyMap
from ..types import StrPath

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'load_database_url_from_config',
    'make_engine',
    # Singletons
    'engine',
    'session',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


DATABASE_URL: Final[str] = (
    os.getenv('DATABASE_URL')
    or os.getenv('DATABASE_DSN')
    or 'sqlite+pysqlite:///:memory:'
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _resolve_url_from_mapping(
    cfg: StrAnyMap,
) -> str | None:
    """
    Return a URL/DSN from a mapping if present.

    Parameters
    ----------
    cfg : StrAnyMap
        Configuration mapping potentially containing connection fields.

    Returns
    -------
    str | None
        Resolved URL/DSN string, if present.
    """
    conn = cfg.get('connection_string') or cfg.get('url') or cfg.get('dsn')
    if isinstance(conn, str) and conn.strip():
        return conn.strip()

    # Some configs nest defaults.
    # E.g., databases: { mssql: { default: {...} } }
    default_cfg = cfg.get('default')
    if isinstance(default_cfg, Mapping):
        return _resolve_url_from_mapping(default_cfg)

    return None


# SECTION: FUNCTIONS ======================================================== #


def load_database_url_from_config(
    path: StrPath,
    *,
    name: str | None = None,
) -> str:
    """
    Extract a database URL/DSN from a YAML/JSON config file.

    The loader is schema-tolerant: it looks for a top-level "databases" map
    and then for a named entry (*name*). Each entry may contain either a
    ``connection_string``/``url``/``dsn`` or a nested ``default`` block with
    those fields.

    Parameters
    ----------
    path : StrPath
        Location of the configuration file.
    name : str | None, optional
        Named database entry under the ``databases`` map (default:
        ``default``).

    Returns
    -------
    str
        Resolved database URL/DSN string.

    Raises
    ------
    KeyError
        If the specified database entry is not found.
    TypeError
        If the config structure is invalid.
    ValueError
        If no connection string/URL/DSN is found for the specified entry.
    """
    cfg = File(Path(path)).read()
    if not isinstance(cfg, Mapping):
        raise TypeError('Database config must be a mapping')

    databases = cfg.get('databases') if isinstance(cfg, Mapping) else None
    if not isinstance(databases, Mapping):
        raise KeyError('Config missing top-level "databases" mapping')

    target = name or 'default'
    entry = databases.get(target)
    if entry is None:
        raise KeyError(f'Database entry "{target}" not found in config')
    if not isinstance(entry, Mapping):
        raise TypeError(f'Database entry "{target}" must be a mapping')

    url = _resolve_url_from_mapping(entry)
    if not url:
        raise ValueError(
            f'Database entry "{target}" lacks connection_string/url/dsn',
        )
    return url


def make_engine(
    url: str | None = None,
    **engine_kwargs: Any,
) -> Engine:
    """
    Create a SQLAlchemy Engine, defaulting to env config if no URL given.

    Parameters
    ----------
    url : str | None, optional
        Database URL/DSN string. When omitted, ``DATABASE_URL`` is used.
    **engine_kwargs : Any
        Extra keyword arguments forwarded to ``create_engine``.

    Returns
    -------
    Engine
        Configured SQLAlchemy engine instance.
    """
    resolved_url = url or DATABASE_URL
    return create_engine(resolved_url, pool_pre_ping=True, **engine_kwargs)


# SECTION: SINGLETONS ======================================================= #


# Default engine/session for callers that rely on module-level singletons.
engine = make_engine()
session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
