"""
:mod:`tests.meta.pytest_meta_support` module.

Shared helpers for meta-level repository guardrail tests.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import yaml

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'canonical_requirement_name',
    'markdown_table_rows',
    'normalized_text',
    'read_lines',
    'read_text',
    'read_toml',
    'read_yaml',
    'regex_matches',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_REQUIREMENT_NAME_PATTERN = re.compile(r'^\s*([A-Za-z0-9_.-]+)')


# SECTION: FUNCTIONS ======================================================== #


def canonical_requirement_name(requirement: str) -> str:
    """
    Return the normalized package name from one requirement string.

    Parameters
    ----------
    requirement : str
        Dependency requirement string.

    Returns
    -------
    str
        Canonical package name with case folded and underscores normalized.

    Raises
    ------
    ValueError
        If no package name can be parsed from the requirement string.
    """
    match = _REQUIREMENT_NAME_PATTERN.match(requirement)
    if match is None:
        raise ValueError(f'Invalid requirement: {requirement!r}')
    return match.group(1).casefold().replace('_', '-')


def markdown_table_rows(
    path: Path,
) -> list[tuple[str, ...]]:
    """
    Return markdown table rows as trimmed cell tuples.

    Parameters
    ----------
    path : Path
        Path to the markdown file.

    Returns
    -------
    list[tuple[str, ...]]
        List of tuples representing the rows of the markdown table.
    """
    return [
        row
        for line in read_lines(path)
        if line.startswith('|')
        if (row := tuple(part.strip() for part in line.split('|')[1:-1]))
    ]


def normalized_text(
    value: str,
) -> str:
    """
    Return case-folded text with Markdown line wrapping normalized.

    Parameters
    ----------
    value : str
        Text to normalize.

    Returns
    -------
    str
        Case-folded text with contiguous whitespace collapsed.
    """
    return ' '.join(value.casefold().split())


def read_lines(
    path: Path,
) -> list[str]:
    """
    Return UTF-8 text content split into lines.

    Parameters
    ----------
    path : Path
        Path to the text file.

    Returns
    -------
    list[str]
        List of lines from the file.
    """
    return read_text(path).splitlines()


def read_text(
    path: Path,
    *,
    errors: str = 'strict',
) -> str:
    """
    Return UTF-8 text content from one file.

    Parameters
    ----------
    path : Path
        Path to the text file.
    errors : str
        Error handling strategy passed to :meth:`Path.read_text`.

    Returns
    -------
    str
        File content decoded as UTF-8.
    """
    return path.read_text(encoding='utf-8', errors=errors)


def read_toml(
    path: Path,
) -> dict[str, Any]:
    """
    Return parsed TOML data from one file.

    Parameters
    ----------
    path : Path
        Path to the TOML file.

    Returns
    -------
    dict[str, Any]
        Parsed TOML content.
    """
    return tomllib.loads(read_text(path))


def read_yaml(
    path: Path,
) -> Any:
    """
    Return parsed YAML data from one file.

    Parameters
    ----------
    path : Path
        Path to the YAML file.

    Returns
    -------
    Any
        Parsed YAML content.
    """
    return yaml.safe_load(read_text(path))


def regex_matches(
    path: Path,
    pattern: re.Pattern[str],
) -> list[re.Match[str]]:
    """
    Return regex matches found from each line in one file.

    Parameters
    ----------
    path : Path
        Path to the text file.
    pattern : re.Pattern[str]
        Regular expression pattern to match against each line.

    Returns
    -------
    list[re.Match[str]]
        List of regex match objects for each line that matches the pattern.
    """
    return [
        match for line in read_lines(path) if (match := pattern.match(line)) is not None
    ]
