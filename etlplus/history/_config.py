"""
:mod:`etlplus.history._config` module.

Configuration helpers for local run-history behavior.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Literal
from typing import Self
from typing import cast

from ..utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'HistoryConfig',
    'ResolvedHistoryConfig',
    # Constants
    'DEFAULT_HISTORY_BACKEND',
    'DEFAULT_STATE_DIR',
    # Functions
    'resolve_history_config',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_VALID_HISTORY_BACKENDS = frozenset({'sqlite', 'jsonl'})


# SECTION: CONSTANTS ======================================================== #


DEFAULT_HISTORY_BACKEND: Literal['sqlite', 'jsonl'] = 'sqlite'
DEFAULT_STATE_DIR = Path('~/.etlplus').expanduser()


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_backend(
    value: object,
) -> Literal['sqlite', 'jsonl'] | None:
    """Return one supported history backend name when valid."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized in _VALID_HISTORY_BACKENDS:
        return cast(Literal['sqlite', 'jsonl'], normalized)
    return None


def _coerce_flag(
    value: object,
    *,
    default: bool,
) -> bool:
    """Return one boolean flag or *default* when the input is invalid."""
    return value if isinstance(value, bool) else default


def _coerce_state_dir(
    value: str | PathLike[str] | None,
) -> Path:
    """Return one normalized history state directory path."""
    if value is None:
        return DEFAULT_STATE_DIR
    return Path(value).expanduser()


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True, frozen=True)
class HistoryConfig:
    """Pipeline-level local history defaults."""

    # -- Instance Attributes -- #

    enabled: bool = True
    backend: Literal['sqlite', 'jsonl'] | None = None
    state_dir: str | None = None
    capture_tracebacks: bool = False

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap | None,
    ) -> Self:
        """Parse one optional history config mapping."""
        if not isinstance(obj, Mapping):
            return cls()

        raw_state_dir = obj.get('state_dir')
        return cls(
            enabled=_coerce_flag(obj.get('enabled'), default=True),
            backend=_coerce_backend(obj.get('backend')),
            state_dir=(
                None
                if raw_state_dir is None
                else raw_state_dir
                if isinstance(raw_state_dir, str)
                else str(raw_state_dir)
            ),
            capture_tracebacks=_coerce_flag(
                obj.get('capture_tracebacks'),
                default=False,
            ),
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class ResolvedHistoryConfig:
    """Resolved local-history settings used at runtime."""

    # -- Instance Attributes -- #

    enabled: bool
    backend: Literal['sqlite', 'jsonl']
    state_dir: Path
    capture_tracebacks: bool


# SECTION: FUNCTIONS ======================================================== #


def resolve_history_config(
    config: HistoryConfig | None,
    *,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
    backend: str | None = None,
    state_dir: str | PathLike[str] | None = None,
    capture_tracebacks: bool | None = None,
) -> ResolvedHistoryConfig:
    """
    Return effective local-history settings from config, env, and CLI.

    Precedence (highest to lowest):
    1. CLI arguments
    2. Environment variables
    3. Pipeline-level config
    4. Internal defaults

    Parameters
    ----------
    config : HistoryConfig | None
        Optional pipeline-level history config.
    env : Mapping[str, str] | None, optional
        Optional environment variables mapping. Defaults to empty mapping.
    enabled : bool | None, optional
        Optional CLI argument to enable or disable history. Defaults to
        ``None``.
    backend : str | None, optional
        Optional CLI argument to specify the history backend. Defaults to
        ``None``.
    state_dir : str | PathLike[str] | None, optional
        Optional CLI argument to specify the state directory. Defaults to
        ``None``.
    capture_tracebacks : bool | None, optional
        Optional CLI argument to enable or disable traceback capture. Defaults to
        ``None``.

    Returns
    -------
    ResolvedHistoryConfig
        Effective local-history settings to use at runtime.
    """
    history_cfg = config if config is not None else HistoryConfig()
    env_map = env or {}

    return ResolvedHistoryConfig(
        enabled=(enabled if enabled is not None else history_cfg.enabled),
        backend=(
            _coerce_backend(backend)
            or _coerce_backend(env_map.get('ETLPLUS_HISTORY_BACKEND'))
            or history_cfg.backend
            or DEFAULT_HISTORY_BACKEND
        ),
        state_dir=_coerce_state_dir(
            state_dir
            or env_map.get('ETLPLUS_STATE_DIR')
            or history_cfg.state_dir,
        ),
        capture_tracebacks=(
            capture_tracebacks
            if capture_tracebacks is not None
            else history_cfg.capture_tracebacks
        ),
    )
