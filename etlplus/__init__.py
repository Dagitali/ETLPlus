"""ETLPlus - A Swiss Army knife for enabling simple ETL operations.

This package provides tools for:
- Extracting data from files, databases, and REST APIs
- Validating data from various sources
- Transforming data
- Loading data to files, databases, and REST APIs
"""

__version__ = "0.1.0"
__author__ = "ETLPlus Team"

from etlplus.extract import extract
from etlplus.validate import validate
from etlplus.transform import transform
from etlplus.load import load

__all__ = ["extract", "validate", "transform", "load"]
