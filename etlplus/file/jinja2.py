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

from typing import Any

from ..utils._types import JSONDict
from ._enums import FileFormat
from ._imports import get_dependency
from .base import TemplateFileHandlerABC
from .base import TemplateTextIOMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Jinja2File',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _jinja2() -> Any:
    """Return the required Jinja2 module."""
    return get_dependency(
        'jinja2',
        format_name='JINJA2',
        pip_name='Jinja2',
        required=True,
    )


# SECTION: CLASSES ========================================================== #


class Jinja2File(TemplateTextIOMixin, TemplateFileHandlerABC):
    """Handler implementation for JINJA2 files."""

    # -- Class Attributes -- #

    format = FileFormat.JINJA2
    template_engine = 'jinja2'

    # -- Instance Methods -- #

    def build_template(
        self,
        jinja2_module: Any,
        template: str,
        *,
        strict_undefined: bool = False,
        trim_blocks: bool = False,
        lstrip_blocks: bool = False,
    ) -> Any:
        """Build one Jinja2 template object for rendering."""
        if not (strict_undefined or trim_blocks or lstrip_blocks):
            return jinja2_module.Template(template)
        env_kwargs: dict[str, object] = {
            'trim_blocks': trim_blocks,
            'lstrip_blocks': lstrip_blocks,
        }
        if strict_undefined:
            env_kwargs['undefined'] = jinja2_module.StrictUndefined
        return jinja2_module.Environment(**env_kwargs).from_string(template)

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
        jinja2 = _jinja2()
        template_obj = self.build_template(
            jinja2,
            template,
            strict_undefined=strict_undefined,
            trim_blocks=trim_blocks,
            lstrip_blocks=lstrip_blocks,
        )
        return template_obj.render(**context)
