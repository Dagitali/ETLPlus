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
from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import normalize_records
from ._io import require_str_key
from ._io import stringify_value
from ._io import write_text
from .base import ReadOptions
from .base import TemplateFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MustacheFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class MustacheFile(TemplateFileHandlerABC):
    """
    Handler implementation for MUSTACHE files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MUSTACHE
    template_engine = 'mustache'

    # -- Instance Methods -- #

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """
        Read and return template payload from *path*.

        Parameters
        ----------
        path : Path
            Path to the MUSTACHE template file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Single-row payload with one ``template`` field.
        """
        encoding = self.encoding_from_read_options(options)
        return [{'template': path.read_text(encoding=encoding)}]

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write template payload to *path* and return row count.

        Parameters
        ----------
        path : Path
            Path to the MUSTACHE template file on disk.
        data : JSONData
            One object (or one-object list) containing a ``template`` string.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            Number of payload rows written.

        Raises
        ------
        TypeError
            If the payload does not contain exactly one object or if the object
            does not contain a string ``template`` field.
        """
        rows = normalize_records(data, self.format_name)
        if not rows:
            return 0
        if len(rows) != 1:
            raise TypeError(
                'MUSTACHE payloads must contain exactly one object',
            )
        template_text = require_str_key(
            rows[0],
            format_name='MUSTACHE',
            key='template',
        )
        write_text(
            path,
            template_text,
            encoding=self.encoding_from_write_options(options),
        )
        return 1

    def render(
        self,
        template: str,
        context: JSONDict,
    ) -> str:
        """
        Render Mustache template text using context data.

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
            key = match.group('key')
            value = context.get(key)
            return stringify_value(value)

        return _MUSTACHE_TOKEN_PATTERN.sub(_replace, template)


# SECTION: INTERNAL CONSTANTS =============================================== #


_MUSTACHE_HANDLER = MustacheFile()

_MUSTACHE_TOKEN_PATTERN = re.compile(
    r'{{\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*}}',
)


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``MustacheFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MUSTACHE file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the MUSTACHE file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _MUSTACHE_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``MustacheFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the MUSTACHE file on disk.
    data : JSONData
        Data to write as MUSTACHE file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the MUSTACHE file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _MUSTACHE_HANDLER.write,
    )
