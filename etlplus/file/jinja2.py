"""
:mod:`etlplus.file.jinja2` module.

Helpers for reading/writing Jinja2 (JINJA2) template files.

Notes
-----
- A JINJA2 file is a text file used for generating HTML or other text formats
    by combining templates with data.
- Common cases:
    - HTML templates.
    - Email templates.
    - Configuration files.
- Rule of thumb:
    - If you need to work with Jinja2 template files, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ._imports import get_dependency
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import normalize_records
from ._io import require_str_key
from ._io import write_text
from .base import ReadOptions
from .base import TemplateFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Jinja2File',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class Jinja2File(TemplateFileHandlerABC):
    """
    Handler implementation for JINJA2 files.
    """

    # -- Class Attributes -- #

    format = FileFormat.JINJA2
    template_engine = 'jinja2'

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
            Path to the JINJA2 template file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONList
            Single-row payload with one ``template`` field.
        """
        encoding = self.encoding_from_read_options(options)
        return [{'template': path.read_text(encoding=encoding)}]

    def render(
        self,
        template: str,
        context: JSONDict,
        *,
        strict_undefined: bool = False,
        trim_blocks: bool = False,
        lstrip_blocks: bool = False,
    ) -> str:
        """
        Render Jinja2 template text using context data.

        Parameters
        ----------
        template : str
            Template text to render.
        context : JSONDict
            Context dictionary for rendering.
        strict_undefined : bool, optional
            Raise when templates reference undefined variables.
        trim_blocks : bool, optional
            Remove the first newline after a block.
        lstrip_blocks : bool, optional
            Strip leading spaces and tabs from block lines.

        Returns
        -------
        str
            Rendered template output.
        """
        jinja2 = get_dependency(
            'jinja2',
            format_name='JINJA2',
            pip_name='Jinja2',
        )
        if strict_undefined or trim_blocks or lstrip_blocks:
            env_kwargs: dict[str, object] = {
                'trim_blocks': trim_blocks,
                'lstrip_blocks': lstrip_blocks,
            }
            if strict_undefined:
                env_kwargs['undefined'] = jinja2.StrictUndefined
            template_obj = jinja2.Environment(**env_kwargs).from_string(
                template,
            )
        else:
            template_obj = jinja2.Template(template)
        return template_obj.render(**context)

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
            Path to the JINJA2 template file on disk.
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
            raise TypeError('JINJA2 payloads must contain exactly one object')
        template_text = require_str_key(
            rows[0],
            format_name='JINJA2',
            key='template',
        )
        write_text(
            path,
            template_text,
            encoding=self.encoding_from_write_options(options),
        )
        return 1


# SECTION: INTERNAL CONSTANTS =============================================== #


_JINJA2_HANDLER = Jinja2File()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONList:
    """
    Deprecated wrapper. Use ``Jinja2File().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the JINJA2 file on disk.

    Returns
    -------
    JSONList
        The list of dictionaries read from the JINJA2 file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _JINJA2_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``Jinja2File().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the JINJA2 file on disk.
    data : JSONData
        Data to write as JINJA2 file. Should be a list of dictionaries or a
        single dictionary.

    Returns
    -------
    int
        The number of rows written to the JINJA2 file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _JINJA2_HANDLER.write,
    )
