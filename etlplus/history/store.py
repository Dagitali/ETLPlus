"""
:mod:`etlplus.history.store` module.

Local run-history persistence backends.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..__version__ import __version__
from ..utils.types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'HISTORY_SCHEMA_VERSION',
    # Classes
    'HistoryStore',
    'JsonlHistoryStore',
    'RunRecord',
    'SQLiteHistoryStore',
    # Functions
    'build_run_record',
    'open_history_store',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_HISTORY_BACKEND = 'sqlite'
_DEFAULT_STATE_DIR = Path('~/.etlplus').expanduser()


# SECTION: CONSTANTS ======================================================== #


HISTORY_SCHEMA_VERSION = 1


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class RunRecord:
    """Persisted metadata for one CLI run invocation."""

    run_id: str
    pipeline_name: str | None
    job_name: str | None
    config_path: str
    config_sha256: str | None
    status: str
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    records_in: int | None
    records_out: int | None
    error_type: str | None
    error_message: str | None
    error_traceback: str | None
    result_summary: JSONData | None
    host: str | None
    pid: int | None
    etlplus_version: str | None


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_state_dir(state_dir: str | os.PathLike[str] | None = None) -> Path:
    raw = state_dir or os.getenv('ETLPLUS_STATE_DIR')
    if not raw:
        return _DEFAULT_STATE_DIR
    return Path(raw).expanduser()


def _config_sha256(config_path: str) -> str | None:
    path = Path(config_path)
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# SECTION: FUNCTIONS ======================================================== #


def build_run_record(
    *,
    run_id: str,
    config_path: str,
    started_at: str,
    pipeline_name: str | None = None,
    job_name: str | None = None,
    status: str = 'running',
) -> RunRecord:
    """
    Build the initial persisted record for one CLI run.

    Parameters
    ----------
    run_id : str
        Stable run identifier.
    config_path : str
        Config path used for the run.
    started_at : str
        Run start timestamp in UTC ISO-8601 form.
    pipeline_name : str | None, optional
        Pipeline name from the config, if known.
    job_name : str | None, optional
        Job name for the invocation, if known.
    status : str, optional
        Initial run status. Default is ``running``.

    Returns
    -------
    RunRecord
        Persistable record for the run.
    """
    return RunRecord(
        run_id=run_id,
        pipeline_name=pipeline_name,
        job_name=job_name,
        config_path=config_path,
        config_sha256=_config_sha256(config_path),
        status=status,
        started_at=started_at,
        finished_at=None,
        duration_ms=None,
        records_in=None,
        records_out=None,
        error_type=None,
        error_message=None,
        error_traceback=None,
        result_summary=None,
        host=socket.gethostname(),
        pid=os.getpid(),
        etlplus_version=__version__,
    )


def open_history_store() -> HistoryStore:
    """
    Open the configured local history backend.

    Returns
    -------
    HistoryStore
        Ready-to-use local history backend.

    Raises
    ------
    ValueError
        If the configured backend name is unsupported.
    """
    backend = os.getenv('ETLPLUS_HISTORY_BACKEND', _DEFAULT_HISTORY_BACKEND)
    state_dir = _coerce_state_dir()
    if backend == 'sqlite':
        return SQLiteHistoryStore(state_dir / 'history.sqlite')
    if backend == 'jsonl':
        return JsonlHistoryStore(state_dir / 'history.jsonl')
    raise ValueError(
        'ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl',
    )


# SECTION: CLASSES ========================================================== #


class HistoryStore:
    """Minimal local history-store interface."""

    def record_run_started(self, record: RunRecord) -> None:
        """Persist the start of a run."""
        raise NotImplementedError

    def record_run_finished(
        self,
        run_id: str,
        *,
        status: str,
        finished_at: str,
        duration_ms: int,
        result_summary: JSONData | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        error_traceback: str | None = None,
    ) -> None:
        """Persist completion or failure details for a run."""
        raise NotImplementedError


class JsonlHistoryStore(HistoryStore):
    """JSONL-backed local run history."""

    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def _append_record(self, payload: dict[str, Any]) -> None:
        with self.log_path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.write('\n')

    def record_run_started(self, record: RunRecord) -> None:
        self._append_record(asdict(record))

    def record_run_finished(
        self,
        run_id: str,
        *,
        status: str,
        finished_at: str,
        duration_ms: int,
        result_summary: JSONData | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        error_traceback: str | None = None,
    ) -> None:
        self._append_record(
            {
                'duration_ms': duration_ms,
                'error_message': error_message,
                'error_traceback': error_traceback,
                'error_type': error_type,
                'finished_at': finished_at,
                'result_summary': result_summary,
                'run_id': run_id,
                'schema_version': HISTORY_SCHEMA_VERSION,
                'status': status,
            },
        )


class SQLiteHistoryStore(HistoryStore):
    """SQLite-backed local run history."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """,
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT,
                    job_name TEXT,
                    config_path TEXT NOT NULL,
                    config_sha256 TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms INTEGER,
                    records_in INTEGER,
                    records_out INTEGER,
                    error_type TEXT,
                    error_message TEXT,
                    error_traceback TEXT,
                    result_summary TEXT,
                    host TEXT,
                    pid INTEGER,
                    etlplus_version TEXT
                )
                """,
            )
            conn.execute(
                """
                INSERT INTO meta (key, value)
                VALUES ('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(HISTORY_SCHEMA_VERSION),),
            )

    def record_run_started(self, record: RunRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs (
                    run_id,
                    pipeline_name,
                    job_name,
                    config_path,
                    config_sha256,
                    status,
                    started_at,
                    finished_at,
                    duration_ms,
                    records_in,
                    records_out,
                    error_type,
                    error_message,
                    error_traceback,
                    result_summary,
                    host,
                    pid,
                    etlplus_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.run_id,
                    record.pipeline_name,
                    record.job_name,
                    record.config_path,
                    record.config_sha256,
                    record.status,
                    record.started_at,
                    record.finished_at,
                    record.duration_ms,
                    record.records_in,
                    record.records_out,
                    record.error_type,
                    record.error_message,
                    record.error_traceback,
                    json.dumps(record.result_summary)
                    if record.result_summary is not None
                    else None,
                    record.host,
                    record.pid,
                    record.etlplus_version,
                ),
            )

    def record_run_finished(
        self,
        run_id: str,
        *,
        status: str,
        finished_at: str,
        duration_ms: int,
        result_summary: JSONData | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        error_traceback: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET
                    status = ?,
                    finished_at = ?,
                    duration_ms = ?,
                    result_summary = ?,
                    error_type = ?,
                    error_message = ?,
                    error_traceback = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    finished_at,
                    duration_ms,
                    json.dumps(result_summary)
                    if result_summary is not None
                    else None,
                    error_type,
                    error_message,
                    error_traceback,
                    run_id,
                ),
            )
