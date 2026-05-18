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
from typing import Protocol
from typing import runtime_checkable

import yaml  # type: ignore[import-untyped]

from ._mapping import MappingParser

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DEFAULT_SECRETS_FILE_ENV_VAR',
    # Data Classes
    'EnvironmentSecretProvider',
    'LocalFileSecretProvider',
    'SecretResolver',
    # Protocols
    'SecretProvider',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_SECRETS_FILE_ENV_VAR: Final[str] = 'ETLPLUS_SECRETS_FILE'


# SECTION: PROTOCOLS ======================================================== #


@runtime_checkable
class SecretProvider(Protocol):
    """Provider contract for resolving one secret key."""

    # -- Attributes -- #

    name: str

    # -- Instance Methods -- #

    def resolve(
        self,
        key: str,
    ) -> object | None:
        """Return the secret value for *key*, or ``None`` when unavailable."""
        raise NotImplementedError


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class EnvironmentSecretProvider:
    """Resolve secret keys from an environment-style mapping."""

    # -- Instance Attributes -- #

    env_map: Mapping[str, object] | None = None
    name: str = 'env'

    # -- Instance Methods -- #

    def resolve(
        self,
        key: str,
    ) -> object | None:
        """Return one environment-backed secret value or ``None``."""
        if not key or self.env_map is None:
            return None
        value = self.env_map.get(key)
        return value if value not in (None, '') else None


@dataclass(frozen=True, slots=True)
class LocalFileSecretProvider:
    """Resolve secret keys from a local JSON/YAML mapping file."""

    # -- Instance Attributes -- #

    env_map: Mapping[str, object] | None = None
    name: str = 'file'
    secrets_file_env_var: str = DEFAULT_SECRETS_FILE_ENV_VAR

    # -- Internal Instance Methods -- #

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

    def resolve(
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
            json.loads(text) if path.suffix.lower() == '.json' else yaml.safe_load(text)
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


@dataclass(frozen=True, slots=True)
class SecretResolver:
    """
    Resolve additive secret tokens with an environment-first provider model.

    Unqualified ``secret:KEY`` tokens resolve through the environment provider.
    The local file provider remains available through explicit
    ``secret:file:path.to.key`` tokens as a compatibility convenience.
    """

    # -- Instance Attributes -- #

    env_map: Mapping[str, object] | None = None
    secrets_file_env_var: str = DEFAULT_SECRETS_FILE_ENV_VAR

    # -- Internal Instance Methods -- #

    def _provider_map(
        self,
    ) -> dict[str, SecretProvider]:
        """Return supported secret providers keyed by token provider name."""
        return {
            'env': EnvironmentSecretProvider(self.env_map),
            'file': LocalFileSecretProvider(
                self.env_map,
                secrets_file_env_var=self.secrets_file_env_var,
            ),
        }

    def _secrets_file_path(
        self,
    ) -> Path | None:
        """Return the configured local secrets file path, if any."""
        return LocalFileSecretProvider(
            self.env_map,
            secrets_file_env_var=self.secrets_file_env_var,
        )._secrets_file_path()

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
        if not key:
            return None

        resolver = self._provider_map().get(provider)
        return None if resolver is None else resolver.resolve(key)
