"""
:mod:`etlplus.runtime.logging` module.

Shared runtime logging policy helpers.

Notes
-----
- CLI-facing commands should keep user payloads on STDOUT and diagnostics on
  STDERR.
- Library/runtime modules should use :mod:`logging` rather than ad-hoc prints
  for diagnostic events.
- The CLI configures logging centrally so future modules share one baseline
  policy instead of each module inventing its own defaults.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from typing import IO
from typing import Final

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'configure_logging',
    'resolve_log_level',
]


# SECTION: CONSTANTS ======================================================== #


_LOG_LEVELS: Final[dict[str, int]] = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}
_DEFAULT_LEVEL_NAME: Final[str] = 'WARNING'
_QUIET_LEVEL_NAME: Final[str] = 'ERROR'
_VERBOSE_LEVEL_NAME: Final[str] = 'INFO'
_LOG_FORMAT: Final[str] = '%(levelname)s %(name)s: %(message)s'


# SECTION: FUNCTIONS ======================================================== #


def resolve_log_level(
    *,
    quiet: bool = False,
    verbose: bool = False,
    env: Mapping[str, str] | None = None,
) -> int:
    """
    Resolve the effective log level for the current runtime.

    Parameters
    ----------
    quiet : bool, optional
        Whether quiet mode is enabled. Quiet mode takes precedence over
        verbose mode. Default is ``False``.
    verbose : bool, optional
        Whether verbose mode is enabled. Default is ``False``.
    env : Mapping[str, str] | None, optional
        Optional environment mapping used instead of :data:`os.environ`.

    Returns
    -------
    int
        A :mod:`logging` level constant.
    """
    env_map = os.environ if env is None else env
    explicit = (env_map.get('ETLPLUS_LOG_LEVEL') or '').strip().upper()
    if explicit in _LOG_LEVELS:
        return _LOG_LEVELS[explicit]
    if quiet:
        return _LOG_LEVELS[_QUIET_LEVEL_NAME]
    if verbose:
        return _LOG_LEVELS[_VERBOSE_LEVEL_NAME]
    return _LOG_LEVELS[_DEFAULT_LEVEL_NAME]


def configure_logging(
    *,
    quiet: bool = False,
    verbose: bool = False,
    stream: IO[str] | None = None,
    force: bool = False,
    env: Mapping[str, str] | None = None,
) -> int:
    """
    Configure the process-wide logging baseline for ETLPlus runtime code.

    Parameters
    ----------
    quiet : bool, optional
        Whether quiet mode is enabled. Default is ``False``.
    verbose : bool, optional
        Whether verbose mode is enabled. Default is ``False``.
    stream : IO[str] | None, optional
        Stream to receive log records. Defaults to :data:`sys.stderr`.
    force : bool, optional
        Whether to override any existing root logging handlers. Default is
        ``False``.
    env : Mapping[str, str] | None, optional
        Optional environment mapping used instead of :data:`os.environ`.

    Returns
    -------
    int
        The effective logging level that was configured.
    """
    level = resolve_log_level(quiet=quiet, verbose=verbose, env=env)
    logging.basicConfig(
        level=level,
        stream=stream or sys.stderr,
        format=_LOG_FORMAT,
        force=force,
    )
    logging.captureWarnings(True)
    return level
