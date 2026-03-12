"""
Prefer exact modules when autosummary names collide with package exports.
"""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from typing import Any

from sphinx.application import Sphinx
from sphinx.ext import autosummary as autosummary_ext
from sphinx.ext.autosummary import generate as autosummary_generate

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #

_ORIGINAL_IMPORT_BY_NAME = autosummary_ext.import_by_name

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _import_by_name_prefer_module(
    name: str,
    prefixes: Sequence[str | None] = (None,),
) -> tuple[str, Any, Any, str]:
    """
    Resolve exact module imports before falling back to autosummary defaults.

    This avoids stubs like ``etlplus.database.engine`` resolving to the
    ``etlplus.database.engine`` singleton exported by the package instead of
    the actual ``etlplus.database.engine`` module.

    Parameters
    ----------
    name : str
        The fully qualified name to import.
    prefixes : Sequence[str | None], optional
        Optional prefixes to try when resolving the name, by default (None,).

    Returns
    -------
    tuple[str, Any, Any, str]
        The resolved name, object, parent, and module name.
    """
    if '.' in name:
        try:
            module = importlib.import_module(name)
        except ImportError:
            pass
        else:
            return name, module, None, name

    return _ORIGINAL_IMPORT_BY_NAME(name, prefixes)


# SECTION: FUNCTIONS ======================================================== #


def setup(
    app: Sphinx,
) -> dict[str, bool]:
    """
    Register the autosummary resolver patch.

    Parameters
    ----------
    app : Sphinx
        The Sphinx application instance.

    Returns
    -------
    dict[str, bool]
        Extension metadata.
    """
    autosummary_ext.import_by_name = _import_by_name_prefer_module
    autosummary_generate.import_by_name = _import_by_name_prefer_module
    return {
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
