"""
etlplus package.

The top-level module defining ``:mod:etlplus``, a package for enabling simple
ETL operations.

Notes
-----
This package provides tools for:

- Extracting data from files, databases, and REST APIs
- Validating data from various sources
- Transforming data
- Loading data to files, databases, and REST APIs

Public API
----------
extract : callable
    Extract data from files, databases, or REST APIs.
load : callable
    Persist data to files, databases, or send to REST APIs.
transform : callable
    Filter, map, select, sort, and aggregate records.
run : callable
    Run ETL jobs defined in pipeline configuration files.
validate : callable
    Validate data against simple schema-like rules.
"""

__version__ = '0.1.0'
__author__ = 'ETLPlus Team'

from .extract import extract
from .load import load
from .run import run
from .transform import transform
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['extract', 'load', 'run', 'transform', 'validate']
