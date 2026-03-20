"""
:mod:`etlplus.runtime.readiness` module.

Runtime readiness checks for the CLI and future execution surfaces.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Final
from typing import Literal
from typing import cast

from .. import __version__
from ..config import Config
from ..file import File
from ..file import FileFormat
from ..utils import deep_substitute
from ..utils import maybe_mapping
from ..utils.types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'build_readiness_report',
]


# SECTION: TYPE ALIASES ===================================================== #


type CheckStatus = Literal['ok', 'warn', 'error', 'skipped']


# SECTION: CONSTANTS ======================================================== #


_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r'\$\{([^}]+)\}')


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_substitution_tokens(
    value: Any,
) -> set[str]:
    """Return unresolved ``${VAR}`` token names found in nested values."""
    tokens: set[str] = set()

    def _walk(node: Any) -> None:
        match node:
            case str():
                for match in _TOKEN_PATTERN.findall(node):
                    tokens.add(match)
            case Mapping():
                for inner in node.values():
                    _walk(inner)
            case list() | tuple() | set() | frozenset() as seq:
                for inner in seq:
                    _walk(inner)
            case _:
                return

    _walk(value)
    return tokens


def _load_raw_config(
    config_path: str,
) -> StrAnyMap:
    """Load raw YAML config and require a mapping root."""
    raw = File(Path(config_path), FileFormat.YAML).read()
    mapping = maybe_mapping(raw)
    if mapping is None:
        raise TypeError('Pipeline YAML must have a mapping/object root')
    return dict(mapping)


def _make_check(
    name: str,
    status: CheckStatus,
    message: str,
    **details: Any,
) -> dict[str, Any]:
    """Return one readiness check row."""
    payload: dict[str, Any] = {
        'name': name,
        'status': status,
        'message': message,
    }
    payload.update(details)
    return payload


def _overall_status(
    checks: list[dict[str, Any]],
) -> Literal['ok', 'warn', 'error']:
    """Return aggregate status from individual check rows."""
    statuses = {cast(CheckStatus, check['status']) for check in checks}
    if 'error' in statuses:
        return 'error'
    if 'warn' in statuses:
        return 'warn'
    return 'ok'


def _supported_python_check() -> dict[str, Any]:
    """Return runtime Python compatibility check."""
    version = (
        f'{sys.version_info.major}.'
        f'{sys.version_info.minor}.'
        f'{sys.version_info.micro}'
    )
    supported = (3, 13) <= sys.version_info[:2] < (3, 15)
    if supported:
        return _make_check(
            'python-version',
            'ok',
            f'Python {version} is within the supported ETLPlus runtime range.',
            version=version,
        )
    return _make_check(
        'python-version',
        'error',
        (
            f'Python {version} is outside the supported ETLPlus runtime '
            'range (>=3.13,<3.15).'
        ),
        version=version,
    )


def _config_checks(
    config_path: str,
    *,
    env: Mapping[str, str] | None,
) -> list[dict[str, Any]]:
    """Return readiness checks for one pipeline config path."""
    checks: list[dict[str, Any]] = []
    path = Path(config_path)
    if not path.exists():
        return [
            _make_check(
                'config-file',
                'error',
                f'Configuration file does not exist: {path}',
                path=str(path),
            ),
        ]

    checks.append(
        _make_check(
            'config-file',
            'ok',
            f'Configuration file exists: {path}',
            path=str(path),
        ),
    )

    raw = _load_raw_config(str(path))
    checks.append(
        _make_check(
            'config-parse',
            'ok',
            'Configuration YAML parsed successfully.',
        ),
    )

    cfg = Config.from_dict(raw)
    base_env = dict(getattr(cfg.profile, 'env', {}) or {})
    external_env = dict(env) if env is not None else dict(os.environ)
    effective_env = base_env | external_env
    resolved = deep_substitute(raw, cfg.vars, effective_env)
    unresolved = sorted(_collect_substitution_tokens(resolved))

    if unresolved:
        checks.append(
            _make_check(
                'config-substitution',
                'error',
                'Configuration still contains unresolved substitution tokens.',
                unresolved_tokens=unresolved,
            ),
        )
        return checks

    Config.from_dict(cast(StrAnyMap, resolved))
    checks.append(
        _make_check(
            'config-substitution',
            'ok',
            'Configuration substitutions resolved successfully.',
        ),
    )
    return checks


# SECTION: FUNCTIONS ======================================================== #


def build_readiness_report(
    *,
    config_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """
    Build a runtime readiness report for the current ETLPlus environment.

    Parameters
    ----------
    config_path : str | None, optional
        Optional pipeline configuration file to validate. Default is ``None``.
    env : Mapping[str, str] | None, optional
        Optional environment mapping used instead of :data:`os.environ`.

    Returns
    -------
    dict[str, Any]
        JSON-serializable readiness report.
    """
    checks: list[dict[str, Any]] = [_supported_python_check()]

    if config_path:
        try:
            checks.extend(_config_checks(config_path, env=env))
        except (OSError, TypeError, ValueError) as exc:
            checks.append(
                _make_check(
                    'config-parse',
                    'error',
                    str(exc),
                    path=config_path,
                ),
            )
    else:
        checks.append(
            _make_check(
                'config-file',
                'skipped',
                'No configuration file provided; only runtime checks were performed.',
            ),
        )

    return {
        'status': _overall_status(checks),
        'etlplus_version': __version__,
        'python_version': (
            f'{sys.version_info.major}.'
            f'{sys.version_info.minor}.'
            f'{sys.version_info.micro}'
        ),
        'checks': checks,
    }
