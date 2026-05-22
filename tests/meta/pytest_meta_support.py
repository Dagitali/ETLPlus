"""
:mod:`tests.meta.pytest_meta_support` module.

Shared helpers for meta-level repository guardrail tests.
"""

from __future__ import annotations

import re
from pathlib import Path

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / 'tests'


# SECTION: TYPES ============================================================ #


type TextSnippetCase = tuple[Path, str]


# SECTION: FUNCTIONS ======================================================== #


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
        List of lines from the file, with leading and trailing whitespace
        removed.
    """
    return path.read_text(encoding='utf-8').splitlines()


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


def text_snippet_case_id(
    case: TextSnippetCase,
) -> str:
    """
    Return a stable pytest ID for a path/snippet guardrail case.

    Parameters
    ----------
    case : TextSnippetCase
        Pair containing the path under test and the expected snippet.

    Returns
    -------
    str
        Stable test-case ID containing the file name and expected snippet.
    """
    path, snippet = case
    return f'{path.name}:{snippet}'
