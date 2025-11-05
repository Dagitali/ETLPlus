"""
etlplus.config.types
====================

A module centralizing type aliases used in the ``:mod:etlplus.config`` package.

Contents
--------
- Type aliases: ``Source``, ``Target``

Examples
--------
>>> from etlplus.config import Source
>>> src: Source = {
>>>     "type": "file",
>>>     "path": "/data/input.csv",
>>> }
>>> from etlplus.config import Target
>>> tgt: Target = {
>>>     "type": "database",
>>>     "connection_string": "postgresql://user:pass@localhost/db",
>>> }
>>> from etlplus.api import RetryPolicy
>>> rp: RetryPolicy = {"max_attempts": 3, "backoff": 0.5}
"""
from __future__ import annotations

from .sources import SourceApi
from .sources import SourceDb
from .sources import SourceFile
from .targets import TargetApi
from .targets import TargetDb
from .targets import TargetFile


# SECTION: EXPORTS  ========================================================= #


__all__ = ['Source', 'Target']


# SECTION: TYPE ALIASES ===================================================== #


type Source = SourceFile | SourceDb | SourceApi
type Target = TargetFile | TargetApi | TargetDb
