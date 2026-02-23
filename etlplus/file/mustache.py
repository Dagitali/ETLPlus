"""
:mod:`etlplus.file.mustache` module.

Helpers for reading/writing Mustache (MUSTACHE) template files.

Notes
-----
- A MUSTACHE file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Mustache template files, use this module for
        reading and writing.
"""

from __future__ import annotations

import re

from ._mixins import RegexTemplateRenderMixin
from .base import TemplateFileHandlerABC
from .base import TemplateTextIOMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MustacheFile',
]


# SECTION: CLASSES ========================================================== #


class MustacheFile(
    RegexTemplateRenderMixin,
    TemplateTextIOMixin,
    TemplateFileHandlerABC,
):
    """
    Handler implementation for MUSTACHE files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MUSTACHE
    template_engine = 'mustache'
    token_pattern = re.compile(r'{{\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*}}')
