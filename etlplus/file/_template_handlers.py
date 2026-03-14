"""
:mod:`etlplus.file._template_handlers` module.

Shared abstractions for regex-token template handlers.
"""

from __future__ import annotations

import re

from ._mixins import RegexTemplateRenderMixin
from .base import TemplateFileHandlerABC
from .base import TemplateTextIOMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'BraceTokenTemplateHandlerMixin',
    'RegexTemplateHandlerMixin',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_BRACE_TOKEN_PATTERN = re.compile(
    r'{{\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*}}',
)


# SECTION: CLASSES ========================================================== #


class RegexTemplateHandlerMixin(
    RegexTemplateRenderMixin,
    TemplateTextIOMixin,
    TemplateFileHandlerABC,
):
    """Shared regex-token template implementation."""


class BraceTokenTemplateHandlerMixin(RegexTemplateHandlerMixin):
    """Shared Handlebars/Mustache-style token implementation."""

    # -- Class Attributes -- #

    token_pattern = _BRACE_TOKEN_PATTERN
