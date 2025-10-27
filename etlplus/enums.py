"""
ETLPlus Enums
=======================

Shared enumeration types used across ETLPlus modules.
"""
from __future__ import annotations

import enum


# SECTION: ENUMS ============================================================ #


class DataConnectorType(enum.StrEnum):
    """
    Supported data connector types.
    """

    FILE = 'file'
    DATABASE = 'database'
    API = 'api'


class FileFormat(enum.StrEnum):
    """
    Supported file formats for extraction.
    """

    CSV = 'csv'
    JSON = 'json'
    XML = 'xml'


class HttpMethod(enum.StrEnum):
    """
    HTTP verbs that accept JSON payloads.
    """

    POST = 'post'
    PUT = 'put'
    PATCH = 'patch'
