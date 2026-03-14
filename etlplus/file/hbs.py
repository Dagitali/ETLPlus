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

from ._template_handlers import BraceTokenTemplateHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HbsFile',
]


# SECTION: CLASSES ========================================================== #


class HbsFile(BraceTokenTemplateHandlerMixin):
    """Handler implementation for HBS files."""

    # -- Class Attributes -- #

    format = FileFormat.HBS
    template_engine = 'handlebars'
