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
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import fields
from pathlib import Path
from typing import Any

from ..__version__ import __version__
from ..file.ndjson import NdjsonFile
from ..file.sqlite import SqliteFile
from ..utils import serialize_json
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
    'iter_history_runs',
    'open_history_store',
]


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

    # -- Class Methods -- #

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        config_path: str,
        started_at: str,
        pipeline_name: str | None = None,
        job_name: str | None = None,
        status: str = 'running',
    ) -> RunRecord:
        """Build the initial persisted record for one CLI run."""
        return cls(
            run_id=run_id,
            pipeline_name=pipeline_name,
            job_name=job_name,
            config_path=config_path,
            config_sha256=cls._config_sha256(config_path),
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

    # -- Static Methods -- #

    @staticmethod
    def _config_sha256(
        config_path: str,
    ) -> str | None:
        """Compute the SHA-256 hash of the config file at *config_path*."""
        path = Path(config_path)
        if not path.exists():
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_HISTORY_BACKEND = 'sqlite'
_DEFAULT_STATE_DIR = Path('~/.etlplus').expanduser()

_RUN_RECORD_FIELDS = tuple(field.name for field in fields(RunRecord))


# SECTION: CONSTANTS ======================================================== #


HISTORY_SCHEMA_VERSION = 1


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
    return RunRecord.build(
        run_id=run_id,
        config_path=config_path,
        started_at=started_at,
        pipeline_name=pipeline_name,
        job_name=job_name,
        status=status,
    )


def iter_history_runs(
    store: HistoryStore,
) -> Iterator[dict[str, Any]]:
    """
    Yield one normalized run record per ``run_id`` from a history backend.

    Parameters
    ----------
    store : HistoryStore
        History backend to read from.

    Yields
    ------
    dict[str, Any]
        One normalized run record for each distinct ``run_id``.
    """
    yield from HistoryStore.iter_runs(store)


def open_history_store() -> HistoryStore:
    """
    Open the configured local history backend.

    Returns
    -------
    HistoryStore
        Ready-to-use local history backend.
    """
    return HistoryStore.from_environment()


# SECTION: CLASSES ========================================================== #


class HistoryStore:
    """Minimal local history-store interface."""

    # -- Class Methods -- #

    @classmethod
    def from_environment(cls) -> HistoryStore:
        """Open the configured local history backend from environment values."""
        backend = os.getenv('ETLPLUS_HISTORY_BACKEND', _DEFAULT_HISTORY_BACKEND)
        state_dir = cls._coerce_state_dir()
        if backend == 'sqlite':
            return SQLiteHistoryStore(state_dir / 'history.sqlite')
        if backend == 'jsonl':
            return JsonlHistoryStore(state_dir / 'history.jsonl')
        raise ValueError(
            'ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl',
        )

    @classmethod
    def iter_runs(
        cls,
        store: HistoryStore,
    ) -> Iterator[dict[str, Any]]:
        """Yield one normalized run record per ``run_id`` from a history backend."""
        merged_by_run_id: dict[str, dict[str, Any]] = {}
        run_order: list[str] = []

        for record in store.iter_records():
            run_id = record.get('run_id')
            if not isinstance(run_id, str) or not run_id:
                continue
            if run_id not in merged_by_run_id:
                merged_by_run_id[run_id] = {}
                run_order.append(run_id)
            cls._merge_record(merged_by_run_id[run_id], record)

        for run_id in run_order:
            merged = merged_by_run_id[run_id]
            yield {field: merged.get(field) for field in _RUN_RECORD_FIELDS}

    # -- Static Methods -- #

    @staticmethod
    def _coerce_state_dir(
        state_dir: str | os.PathLike[str] | None = None,
    ) -> Path:
        """Coerce a state directory path from a value or environment variable."""
        raw = state_dir or os.getenv('ETLPLUS_STATE_DIR')
        if not raw:
            return _DEFAULT_STATE_DIR
        return Path(raw).expanduser()

    @staticmethod
    def _merge_record(
        record: dict[str, Any],
        update: Mapping[str, Any],
    ) -> None:
        """Merge a partial history update into an accumulated run record."""
        for key, value in update.items():
            if value is not None or key not in record:
                record[key] = value

    # -- Instance Methods -- #

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """
        Yield persisted history records in backend-native form.

        Returns
        -------
        Iterator[dict[str, Any]]
            Stream of persisted history records.

        Raises
        ------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        raise NotImplementedError

    def record_run_started(
        self,
        record: RunRecord,
    ) -> None:
        """
        Persist the start of a run.

        Parameters
        ----------
        record : RunRecord
            Initial run record to persist.

        Raises
        ------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
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
        """
        Persist completion or failure details for a run.

        Parameters
        ----------
        run_id : str
            Stable run identifier.
        status : str
            Final run status, e.g. ``success`` or ``failure``.
        finished_at : str
            Run finish timestamp in UTC ISO-8601 form.
        duration_ms : int
            Run duration in milliseconds.
        result_summary : JSONData | None, optional
            Optional JSON-serializable summary of the run result, e.g. record
            counts or sample output.
        error_type : str | None, optional
            Optional error type name if the run failed.
        error_message : str | None, optional
            Optional error message if the run failed.
        error_traceback : str | None, optional
            Optional error traceback if the run failed.

        Raises
        ------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        raise NotImplementedError


class JsonlHistoryStore(HistoryStore):
    """JSONL-backed local run history."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        log_path: Path,
    ) -> None:
        self._ndjson_file = NdjsonFile()
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    # -- Internal Instance Methods -- #

    def _append_record(
        self,
        payload: dict[str, Any],
    ) -> None:
        """Append a record to the JSONL log file."""
        with self.log_path.open('a', encoding='utf-8') as handle:
            handle.write(self._serialize_record(payload))

    def _serialize_record(
        self,
        payload: dict[str, Any],
    ) -> str:
        """Serialize one history record as a single NDJSON line."""
        return self._ndjson_file.dump_line(payload)

    # -- Instance Methods -- #

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Yield JSONL history records by streaming the log file line by line."""
        if not self.log_path.exists():
            return
        with self.log_path.open('r', encoding='utf-8') as handle:
            for idx, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                yield self._ndjson_file.load_line(stripped, line_number=idx)

    def record_run_started(
        self,
        record: RunRecord,
    ) -> None:
        """
        Persist the start of a run by appending a record to the log.

        Parameters
        ----------
        record : RunRecord
            Initial run record to persist.
        """
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
        """
        Persist completion or failure details for a run by appending a record
        to the log.

        Parameters
        ----------
        run_id : str
            Stable run identifier.
        status : str
            Final run status, e.g. ``success`` or ``failure``.
        finished_at : str
            Run finish timestamp in UTC ISO-8601 form.
        duration_ms : int
            Run duration in milliseconds.
        result_summary : JSONData | None, optional
            Optional JSON-serializable summary of the run result, e.g. record
            counts or sample output.
        error_type : str | None, optional
            Optional error type name if the run failed.
        error_message : str | None, optional
            Optional error message if the run failed.
        error_traceback : str | None, optional
            Optional error traceback if the run failed.
        """
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

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        db_path: Path,
    ) -> None:
        self._sqlite_file = SqliteFile()
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    # -- Internal Instance Methods -- #

    def _connect(self) -> sqlite3.Connection:
        """Create a new SQLite connection."""
        return self._sqlite_file.connect(self.db_path)

    def _ensure_schema(self) -> None:
        """Ensure the database schema is created and up-to-date."""
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

    # -- Instance Methods -- #

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Yield persisted SQLite run rows as dictionaries."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
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
                FROM runs
                ORDER BY started_at ASC, run_id ASC
                """,
            )
            for row in rows:
                payload = dict(row)
                if payload['result_summary'] is not None:
                    payload['result_summary'] = json.loads(payload['result_summary'])
                yield payload

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
        """
        Record completion or failure details for a run.

        Parameters
        ----------
        run_id : str
            Unique identifier for the run.
        status : str
            Final status of the run.
        finished_at : str
            Timestamp when the run finished.
        duration_ms : int
            Duration of the run in milliseconds.
        result_summary : JSONData | None, optional
            Summary of the run results, if available.
        error_type : str | None, optional
            Type of error encountered, if any.
        error_message : str | None, optional
            Error message, if any.
        error_traceback : str | None, optional
            Error traceback, if any.
        """
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
                    (
                        serialize_json(result_summary)
                        if result_summary is not None
                        else None
                    ),
                    error_type,
                    error_message,
                    error_traceback,
                    run_id,
                ),
            )

    def record_run_started(
        self,
        record: RunRecord,
    ) -> None:
        """
        Record the start of a run.

        Parameters
        ----------
        record : RunRecord
            Initial run record to persist.
        """
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
                    serialize_json(record.result_summary)
                    if record.result_summary is not None
                    else None,
                    record.host,
                    record.pid,
                    record.etlplus_version,
                ),
            )
