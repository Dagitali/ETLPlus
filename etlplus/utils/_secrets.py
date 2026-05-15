"""
:mod:`etlplus.utils._secrets` module.

Incremental secret-resolution helpers for config substitution.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Final

import yaml  # type: ignore[import-untyped]

from ._mapping import MappingParser

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DEFAULT_SECRETS_FILE_ENV_VAR',
    # Data Classes
    'SecretResolver',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_SECRETS_FILE_ENV_VAR: Final[str] = 'ETLPLUS_SECRETS_FILE'


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class SecretResolver:
    """Resolve additive secret tokens from environment or a local file."""

    # -- Instance Attributes -- #

    env_map: Mapping[str, object] | None = None
    secrets_file_env_var: str = DEFAULT_SECRETS_FILE_ENV_VAR

    # -- Internal Instance Methods -- #

    def _resolve_env_secret(
        self,
        key: str,
    ) -> object | None:
        """Return one environment-backed secret value or ``None``."""
        if not key or self.env_map is None:
            return None
        value = self.env_map.get(key)
        return value if value not in (None, '') else None

    def _resolve_file_secret(
        self,
        key: str,
    ) -> object | None:
        """Return one local-file-backed secret value or ``None``."""
        if not key:
            return None
        secrets_path = self._secrets_file_path()
        if secrets_path is None:
            return None
        payload = self._load_mapping_file(secrets_path)
        if payload is None:
            return None
        return self._lookup_mapping_key(payload, key)

    def _secrets_file_path(
        self,
    ) -> Path | None:
        """Return the configured local secrets file path, if any."""
        if self.env_map is None:
            return None
        raw_path = self.env_map.get(self.secrets_file_env_var)
        if not isinstance(raw_path, str) or not raw_path:
            return None
        return Path(raw_path).expanduser()

    # -- Instance Methods -- #

    def resolve_token(
        self,
        token_name: str,
    ) -> object | None:
        """Return the resolved value for one ``secret:...`` token."""
        if not token_name.startswith('secret:'):
            return None

        provider, separator, key = token_name.removeprefix('secret:').partition(':')
        provider, key = ('env', provider) if not separator else (provider, key)

        match provider:
            case 'env':
                return self._resolve_env_secret(key)
            case 'file':
                return self._resolve_file_secret(key)
            case _:
                return None

    # -- Static Methods -- #

    @staticmethod
    def _load_mapping_file(
        path: Path,
    ) -> dict[str, Any] | None:
        """Load one JSON or YAML mapping file used for local secret lookup."""
        if not path.exists() or not path.is_file():
            return None

        text = path.read_text(encoding='utf-8')
        payload = (
            json.loads(text)
            if path.suffix.lower() == '.json'
            else yaml.safe_load(text)
        )

        return MappingParser.to_dict(payload) if isinstance(payload, Mapping) else None

    @staticmethod
    def _lookup_mapping_key(
        payload: Mapping[str, Any],
        key: str,
    ) -> object | None:
        """Return one dotted lookup from a nested mapping payload."""
        current: object = payload
        for part in key.split('.'):
            if not isinstance(current, Mapping) or part not in current:
                return None
            current = current[part]
        return current if current not in (None, '') else None
