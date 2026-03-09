# conf.py
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

etlplus = importlib.import_module('etlplus')

# -- Project Information -- #
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'ETLPlus'
copyright = '2026, Dagitali LLC'
author = etlplus.__author__
release = etlplus.__version__

# -- General Configuration -- #
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions: list[str] = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']
exclude_patterns: list[str] = [
    '_build',
    'Thumbs.db',
    '.DS_Store',
]

autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_ignore_module_all = False

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'show-inheritance': True,
}
autodoc_member_order = 'bysource'
autodoc_mock_imports = [
    'click',
    'pydantic',
    'requests',
    'sqlalchemy',
    'typer',
]

# -- Napoleon Configuration -- #

napoleon_google_docstring = False
napoleon_numpy_docstring = True

# -- Options for HTML Output -- #
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_title = f'{project} {release}'
