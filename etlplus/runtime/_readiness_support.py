"""
:mod:`etlplus.runtime._readiness_support` module.

Internal shared types and constants for runtime readiness checks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final
from typing import Literal

from ..utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    '_AWS_ENV_HINTS',
    '_AZURE_STORAGE_BOOTSTRAP_ENV',
    '_AZURE_STORAGE_CREDENTIAL_ENV',
    '_FORMAT_EXTRA_REQUIREMENTS',
    '_SCHEME_EXTRA_REQUIREMENTS',
    '_SUPPORTED_PYTHON_RANGE',
    '_TOKEN_PATTERN',
    # Classes
    '_RequirementSpec',
    '_ResolvedConfigContext',
]


# SECTION: TYPE ALIASES ===================================================== #


type CheckStatus = Literal['ok', 'warn', 'error', 'skipped']


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _RequirementSpec:
    """One optional runtime dependency requirement."""

    # -- Instance Methods -- #

    modules: tuple[str, ...]
    package: str
    extra: str | None = None


@dataclass(frozen=True, slots=True)
class _ResolvedConfigContext:
    """Resolved config state reused across config readiness checks."""

    # -- Instance Methods -- #

    raw: StrAnyMap
    effective_env: dict[str, str]
    unresolved_tokens: list[str]
    resolved_raw: StrAnyMap
    resolved_cfg: object | None


# SECTION: INTERNAL CONSTANTS =============================================== #


_AWS_ENV_HINTS: Final[tuple[str, ...]] = (
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SESSION_TOKEN',
    'AWS_PROFILE',
    'AWS_DEFAULT_PROFILE',
    'AWS_ROLE_ARN',
    'AWS_WEB_IDENTITY_TOKEN_FILE',
    'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
    'AWS_CONTAINER_CREDENTIALS_FULL_URI',
    'AWS_SHARED_CREDENTIALS_FILE',
    'AWS_CONFIG_FILE',
)
_AZURE_STORAGE_BOOTSTRAP_ENV: Final[tuple[str, ...]] = (
    'AZURE_STORAGE_CONNECTION_STRING',
    'AZURE_STORAGE_ACCOUNT_URL',
)
_AZURE_STORAGE_CREDENTIAL_ENV: Final[str] = 'AZURE_STORAGE_CREDENTIAL'

_SUPPORTED_PYTHON_RANGE: Final[tuple[tuple[int, int], tuple[int, int]]] = (
    (3, 13),
    (3, 15),
)
_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r'\$\{([^}]+)\}')

_FORMAT_EXTRA_REQUIREMENTS: Final[dict[str, _RequirementSpec]] = {
    'dta': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'hdf5': _RequirementSpec(('tables',), 'tables'),
    'rda': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'rds': _RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'sav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'zsav': _RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
}
_SCHEME_EXTRA_REQUIREMENTS: Final[dict[str, _RequirementSpec]] = {
    'abfs': _RequirementSpec(
        ('azure.storage.filedatalake',),
        'azure-storage-file-datalake',
        'storage',
    ),
    'azure-blob': _RequirementSpec(
        ('azure.storage.blob',),
        'azure-storage-blob',
        'storage',
    ),
    's3': _RequirementSpec(('boto3',), 'boto3', 'storage'),
}
