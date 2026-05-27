"""
:mod:`tests.meta.test_m_repository_headers` module.

Guardrails for repository-maintenance file header conventions.
"""

from __future__ import annotations

import re

import pytest

from tests.pytest_shared_support import REPO_ROOT

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_HEADER_SECTION_FILES = [
    '.coveragerc',
    '.editorconfig',
    '.flake8',
    '.gitattributes',
    '.github/dependabot.yml',
    '.gitignore',
    '.pre-commit-config.yaml',
    '.ruff.toml',
    'MANIFEST.in',
    'Makefile',
    'pytest.ini',
]


_GITHUB_ACTION_FILES = sorted(
    path.relative_to(REPO_ROOT).as_posix()
    for path in (REPO_ROOT / '.github' / 'actions').glob('*/action.yml')
)


_GITHUB_WORKFLOW_FILES = sorted(
    path.relative_to(REPO_ROOT).as_posix()
    for path in (REPO_ROOT / '.github' / 'workflows').glob('*.yml')
)


_MAINTENANCE_HEADER_FILES = [
    *_HEADER_SECTION_FILES,
    *_GITHUB_ACTION_FILES,
    *_GITHUB_WORKFLOW_FILES,
]


_SPECIAL_REFERENCE_FILES = [
    '.github/FUNDING.yml',
    '.github/release.yml',
]


_REQUIRED_HEADER_SECTIONS = [
    'Responsibilities',
    'Maintainer Notes',
    'References',
]


_REFERENCE_LABEL_PATTERN = re.compile(
    r'^# - .+ '
    r'(?:documentation|reference|specification|guide|examples|repository|service): '
    r'https?://',
)


_REFERENCE_SECTION_PATTERN = re.compile(
    r'^# References$'
    r'(?P<section>(?:\n# - .+)+)',
    re.MULTILINE,
)


_REFERENCE_SECTION_FILES = sorted(
    [
        *_MAINTENANCE_HEADER_FILES,
        *_SPECIAL_REFERENCE_FILES,
    ],
)


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize('relative_path', _MAINTENANCE_HEADER_FILES, ids=str)
def test_repository_maintenance_headers_use_standard_sections(
    relative_path: str,
) -> None:
    """
    Test that repository-maintenance files use the standard header sections.
    """
    text = (REPO_ROOT / relative_path).read_text(encoding='utf-8')

    missing = [
        section for section in _REQUIRED_HEADER_SECTIONS if f'# {section}' not in text
    ]

    assert not missing, f'{relative_path} is missing header sections: {missing}'


@pytest.mark.parametrize('relative_path', _REFERENCE_SECTION_FILES, ids=str)
def test_repository_maintenance_references_are_labeled(
    relative_path: str,
) -> None:
    """
    Test that reference entries use a standard descriptive label suffix.
    """
    offenders = [
        line
        for match in _REFERENCE_SECTION_PATTERN.finditer(
            (REPO_ROOT / relative_path).read_text(
                encoding='utf-8',
                errors='ignore',
            ),
        )
        for line in match.group('section').splitlines()
        if line
        if not _REFERENCE_LABEL_PATTERN.match(line)
    ]

    assert not offenders, (
        f'{relative_path} has nonstandard references:\n- ' + '\n- '.join(offenders)
    )
