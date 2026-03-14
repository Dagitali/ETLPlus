"""
:mod:`etlplus.file.vm` module.

Helpers for reading/writing Apache Velocity (VM) template files.

Notes
-----
- A VM file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Apache Velocity template files, use this module
        for reading and writing.
"""

from __future__ import annotations

import re

from ._template_handlers import RegexTemplateHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'VmFile',
]


# SECTION: CLASSES ========================================================== #


class VmFile(RegexTemplateHandlerMixin):
    """Handler implementation for VM files."""

    # -- Class Attributes -- #

    format = FileFormat.VM
    template_engine = 'velocity'
    token_pattern = re.compile(
        r'\$\{(?P<brace_key>[A-Za-z_][A-Za-z0-9_]*)\}'
        r'|\$(?P<plain_key>[A-Za-z_][A-Za-z0-9_]*)',
    )

    # -- Instance Methods -- #

    def template_key_from_match(
        self,
        match: re.Match[str],
    ) -> str | None:
        """
        Resolve one Velocity token key from a regex match.

        Parameters
        ----------
        match : re.Match[str]
            The regex match object containing Velocity token groups.

        Returns
        -------
        str | None
            The resolved Velocity token key, or None if no key is found.
        """
        return match.group('brace_key') or match.group('plain_key')
