"""
:mod:`etlplus.file.hbs` module.

Helpers for reading/writing Handlebars (HBS) template files.

Notes
-----
- A Handlebars (HBS) template file is a text file used for generating HTML or
    other text formats by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Handlebars template files, use this module for
        reading and writing.
"""

from __future__ import annotations

import re

from ..types import JSONData
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._mixins import RegexTemplateRenderMixin
from .base import TemplateFileHandlerABC
from .base import TemplateTextIOMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HbsFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class HbsFile(
    RegexTemplateRenderMixin,
    TemplateTextIOMixin,
    TemplateFileHandlerABC,
):
    """
    Handler implementation for HBS files.
    """

    # -- Class Attributes -- #

    format = FileFormat.HBS
    template_engine = 'handlebars'
    token_pattern = re.compile(r'{{\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*}}')


# SECTION: INTERNAL CONSTANTS =============================================== #


_HBS_HANDLER = HbsFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``HbsFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the HBS file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _HBS_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``HbsFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the HBS file on disk.
    data : JSONData
        Data to write as HBS file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the HBS file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _HBS_HANDLER.write,
    )
