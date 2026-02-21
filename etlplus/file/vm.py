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

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import stringify_value
from .base import TemplateFileHandlerABC
from .base import TemplateTextIOMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'VmFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class VmFile(TemplateTextIOMixin, TemplateFileHandlerABC):
    """
    Handler implementation for VM files.
    """

    # -- Class Attributes -- #

    format = FileFormat.VM
    template_engine = 'velocity'

    # -- Instance Methods -- #

    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Render VM template text using context data.

        Parameters
        ----------
        template : str
            Template text to render.
        context : JSONDict
            Context dictionary for rendering.

        Returns
        -------
        str
            Rendered template output.
        """

        def _replace(match: re.Match[str]) -> str:
            key = match.group('brace_key') or match.group('plain_key')
            value = context.get(str(key))
            return stringify_value(value)

        return _VM_TOKEN_PATTERN.sub(_replace, template)


# SECTION: INTERNAL CONSTANTS =============================================== #


_VM_TOKEN_PATTERN = re.compile(
    r'\$\{(?P<brace_key>[A-Za-z_][A-Za-z0-9_]*)\}'
    r'|\$(?P<plain_key>[A-Za-z_][A-Za-z0-9_]*)',
)

_VM_HANDLER = VmFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``VmFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the VM file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the VM file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _VM_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``VmFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the VM file on disk.
    data : JSONData
        Data to write as VM file. Should be a list of dictionaries or a single
        dictionary.

    Returns
    -------
    int
        The number of rows written to the VM file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _VM_HANDLER.write,
    )
