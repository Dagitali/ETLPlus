"""
:mod:`tests.unit.meta.pytest_meta_support` module.

Shared helpers for meta-level repository guardrail tests.
"""

from __future__ import annotations

import re
from pathlib import Path

# SECTION: INTERNAL CONSTANTS =============================================== #


REPO_ROOT = Path(__file__).resolve().parents[3]
TESTS_ROOT = REPO_ROOT / 'tests'


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
    rows: list[tuple[str, ...]] = []
    for line in read_lines(path):
        if not line.startswith('|'):
            continue
        row = tuple(part.strip() for part in line.split('|')[1:-1])
        if row:
            rows.append(row)
    return rows


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
    matches: list[re.Match[str]] = []
    for line in read_lines(path):
        if (match := pattern.match(line)) is not None:
            matches.append(match)
    return matches


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


def sorted_glob(
    root: Path,
    pattern: str,
) -> list[Path]:
    """
    Return deterministic ``glob`` matches.

    Parameters
    ----------
    root : Path
        Root directory to start the glob search.
    pattern : str
        Glob pattern to match files.

    Returns
    -------
    list[Path]
        Sorted list of paths matching the glob pattern.
    """
    return sorted(root.glob(pattern))


def sorted_rglob(
    root: Path,
    pattern: str,
) -> list[Path]:
    """
    Return deterministic ``rglob`` matches.

    Parameters
    ----------
    root : Path
        Root directory to start the recursive glob search.
    pattern : str
        Glob pattern to match files.

    Returns
    -------
    list[Path]
        Sorted list of paths matching the recursive glob pattern.
    """
    return sorted(root.rglob(pattern))


def scope_conftests(
    scope_name: str,
) -> list[Path]:
    """
    Return sorted ``conftest.py`` paths for one test scope.

    Parameters
    ----------
    scope_name : str
        Name of the test scope (e.g., 'unit', 'integration').

    Returns
    -------
    list[Path]
        Sorted list of paths to ``conftest.py`` files within the specified test
        scope.
    """
    return sorted_rglob(TESTS_ROOT / scope_name, 'conftest.py')
