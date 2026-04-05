"""
:mod:`etlplus.history._store` module.

Local run-history persistence backends.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
from abc import ABC
from abc import abstractmethod
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from dataclasses import fields
from pathlib import Path
from typing import Any
from typing import Self
from typing import cast

from ..__version__ import __version__
from ..file.ndjson import NdjsonFile
from ..file.sqlite import SqliteFile
from ..utils import serialize_json
from ..utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HistoryStore',
    'JobRunRecord',
    'JsonlHistoryStore',
    'RunCompletion',
    'RunRecord',
    'RunState',
    'SQLiteHistoryStore',
    # Constants
    'HISTORY_SCHEMA_VERSION',
    # Functions
    'build_run_record',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _deserialize_result_summary(
    result_summary: str | None,
) -> JSONData | None:
    """Deserialize one optional persisted JSON result summary."""
    if result_summary is None:
        return None
    return json.loads(result_summary)


def _deserialize_string_list(
    payload: str | None,
) -> list[str] | None:
    """Deserialize one optional JSON string list."""
    if payload is None:
        return None
    values = json.loads(payload)
    if not isinstance(values, list):
        return None
    return [value for value in values if isinstance(value, str)]


def _file_sha256(
    file_path: str,
) -> str | None:
    """Return the SHA-256 digest for *file_path* when the file exists."""
    path = Path(file_path)
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _serialize_result_summary(
    result_summary: JSONData | None,
) -> str | None:
    """Serialize one optional JSON result summary for persistence."""
    if result_summary is None:
        return None
    return serialize_json(result_summary)


def _serialize_string_list(
    values: list[str] | None,
) -> str | None:
    """Serialize one optional string list for persistence."""
    if values is None:
        return None
    return serialize_json(values)


def _sqlite_job_run_payload(
    record: JobRunRecord,
) -> dict[str, Any]:
    """Return one SQLite-ready payload for a persisted job-run record."""
    payload = record.to_payload()
    payload['result_summary'] = _serialize_result_summary(record.result_summary)
    payload['skipped_due_to'] = _serialize_string_list(record.skipped_due_to)
    return payload


def _sqlite_record_payload(
    record: RunRecord,
) -> dict[str, Any]:
    """Return one SQLite-ready payload for a started run record."""
    payload = record.to_payload()
    payload['result_summary'] = _serialize_result_summary(record.state.result_summary)
    return payload


def _sqlite_row_payload(
    row: sqlite3.Row,
) -> dict[str, Any]:
    """Return one SQLite row decoded into a history payload."""
    payload = dict(row)
    if 'result_summary' in payload:
        payload['result_summary'] = _deserialize_result_summary(
            cast(str | None, payload['result_summary']),
        )
    if 'skipped_due_to' in payload:
        payload['skipped_due_to'] = _deserialize_string_list(
            cast(str | None, payload['skipped_due_to']),
        )
    return payload


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True, frozen=True)
class JobRunRecord:
    """
    Persisted metadata for one executed job inside a DAG-style run.

    Attributes
    ----------
    run_id : str
        Stable parent run identifier.
    job_name : str
        Executed job name.
    pipeline_name : str | None
        Optional pipeline name from the config.
    sequence_index : int
        Zero-based execution-plan position for the job.
    started_at : str | None
        Job start timestamp in UTC ISO-8601 form.
    finished_at : str | None
        Job finish timestamp in UTC ISO-8601 form.
    duration_ms : int | None
        Job duration in milliseconds.
    records_in : int | None
        Optional number of records read by the job.
    records_out : int | None
        Optional number of records written by the job.
    status : str
        Persisted terminal status for the job.
    result_status : str | None
        Optional downstream operation status returned by the job result.
    error_type : str | None
        Optional error type if the job failed.
    error_message : str | None
        Optional error message if the job failed.
    skipped_due_to : list[str] | None
        Optional list of upstream job names that blocked this job.
    result_summary : JSONData | None
        Optional JSON-serializable summary of the job result.
    """

    # -- Instance Attributes -- #

    run_id: str
    job_name: str
    pipeline_name: str | None
    sequence_index: int
    started_at: str | None
    finished_at: str | None
    duration_ms: int | None
    records_in: int | None
    records_out: int | None
    status: str
    result_status: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    skipped_due_to: list[str] | None = None
    result_summary: JSONData | None = None

    # -- Instance Methods -- #

    def to_payload(self) -> dict[str, Any]:
        """Return the flat persisted representation of the job run."""
        return {
            'run_id': self.run_id,
            'job_name': self.job_name,
            'pipeline_name': self.pipeline_name,
            'sequence_index': self.sequence_index,
            'started_at': self.started_at,
            'finished_at': self.finished_at,
            'duration_ms': self.duration_ms,
            'records_in': self.records_in,
            'records_out': self.records_out,
            'status': self.status,
            'result_status': self.result_status,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'skipped_due_to': self.skipped_due_to,
            'result_summary': self.result_summary,
        }


@dataclass(slots=True)
class RunCompletion:
    """
    Persisted completion details for one CLI run invocation.

    Attributes
    ----------
    run_id : str
        Stable run identifier.
    state : RunState
        Shared run state including status, timing, result summary, and errors.
    """

    # -- Instance Attributes -- #

    run_id: str
    state: RunState

    # -- Instance Methods -- #

    def to_payload(self) -> dict[str, Any]:
        """Return the flat persisted representation of the completion."""
        return {'run_id': self.run_id} | self.state.to_payload()


@dataclass(slots=True)
class RunRecord:
    """
    Persisted metadata for one CLI run invocation.

    Attributes
    ----------
    run_id : str
        Stable run identifier.
    pipeline_name : str | None
        Optional pipeline name from the config.
    job_name : str | None
        Optional job name for the invocation.
    config_path : str
        Config path used for the run.
    config_sha256 : str | None
        Optional SHA-256 hash of the config file.
    started_at : str
        Run start timestamp in UTC ISO-8601 form.
    records_in : int | None
        Optional number of records read by the run.
    records_out : int | None
        Optional number of records written by the run.
    state : RunState
        Shared run state including status, timing, result summary, and errors.
    host : str | None
        Optional hostname where the run was executed.
    pid : int | None
        Optional process ID of the run.
    etlplus_version : str | None
        Optional ETLPlus version used for the run.
    """

    # -- Instance Attributes -- #

    run_id: str
    pipeline_name: str | None
    job_name: str | None
    config_path: str
    config_sha256: str | None
    started_at: str
    records_in: int | None
    records_out: int | None
    state: RunState
    host: str | None
    pid: int | None
    etlplus_version: str | None

    # -- Instance Methods -- #

    def to_payload(self) -> dict[str, Any]:
        """Return the flat persisted representation of the run record."""
        return {
            'run_id': self.run_id,
            'pipeline_name': self.pipeline_name,
            'job_name': self.job_name,
            'config_path': self.config_path,
            'config_sha256': self.config_sha256,
            'started_at': self.started_at,
            'records_in': self.records_in,
            'records_out': self.records_out,
            'host': self.host,
            'pid': self.pid,
            'etlplus_version': self.etlplus_version,
        } | self.state.to_payload()

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
            config_sha256=_file_sha256(config_path),
            started_at=started_at,
            records_in=None,
            records_out=None,
            state=RunState.running(status=status),
            host=socket.gethostname(),
            pid=os.getpid(),
            etlplus_version=__version__,
        )


@dataclass(slots=True, frozen=True)
class RunState:
    """
    Shared terminal state for persisted run metadata.

    Attributes
    ----------
    status : str
        Final run status, e.g. ``success`` or ``failure``.
    finished_at : str | None
        Optional run finish timestamp in UTC ISO-8601 form.
    duration_ms : int | None
        Optional run duration in milliseconds.
    result_summary : JSONData | None
        Optional JSON-serializable summary of the run result.
    error_type : str | None
        Optional error type if the run failed.
    error_message : str | None
        Optional error message if the run failed.
    error_traceback : str | None
        Optional error traceback if the run failed.
    """

    # -- Instance Attributes -- #

    status: str
    finished_at: str | None
    duration_ms: int | None
    result_summary: JSONData | None = None
    error_type: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None

    # -- Class Methods -- #

    @classmethod
    def running(
        cls,
        *,
        status: str = 'running',
    ) -> Self:
        """Return the default in-flight state for a run."""
        return cls(
            status=status,
            finished_at=None,
            duration_ms=None,
        )

    # -- Instance Methods -- #

    def to_payload(self) -> dict[str, Any]:
        """Return the flat persisted representation of the outcome."""
        return {
            'status': self.status,
            'finished_at': self.finished_at,
            'duration_ms': self.duration_ms,
            'result_summary': self.result_summary,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'error_traceback': self.error_traceback,
        }


# SECTION: INTERNAL CONSTANTS =============================================== #


_DEFAULT_HISTORY_BACKEND = 'sqlite'
_DEFAULT_STATE_DIR = Path('~/.etlplus').expanduser()

_RUN_STATE_FIELDS = tuple(field.name for field in fields(RunState))
_RUN_RECORD_FIELDS = (
    *(field.name for field in fields(RunRecord) if field.name != 'state'),
    *_RUN_STATE_FIELDS,
)
_JOB_RUN_RECORD_FIELDS = tuple(field.name for field in fields(JobRunRecord))
_RUN_DB_COLUMNS = (
    'run_id',
    'pipeline_name',
    'job_name',
    'config_path',
    'config_sha256',
    'status',
    'started_at',
    'finished_at',
    'duration_ms',
    'records_in',
    'records_out',
    'error_type',
    'error_message',
    'error_traceback',
    'result_summary',
    'host',
    'pid',
    'etlplus_version',
)
_RUN_DB_COLUMNS_SQL = ',\n                    '.join(_RUN_DB_COLUMNS)
_RUN_DB_PLACEHOLDERS = ', '.join('?' for _ in _RUN_DB_COLUMNS)
_JOB_RUN_DB_COLUMNS = _JOB_RUN_RECORD_FIELDS
_JOB_RUN_DB_COLUMNS_SQL = ',\n                    '.join(_JOB_RUN_DB_COLUMNS)
_JOB_RUN_DB_PLACEHOLDERS = ', '.join('?' for _ in _JOB_RUN_DB_COLUMNS)


# SECTION: CONSTANTS ======================================================== #


HISTORY_SCHEMA_VERSION = 2


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


# SECTION: ABSTRACT BASE CLASSES ============================================ #


class HistoryStore(ABC):
    """Minimal local history-store interface."""

    # -- Class Methods -- #

    @classmethod
    def from_environment(cls) -> HistoryStore:
        """Open the configured local history backend from environment values."""
        backend = os.getenv('ETLPLUS_HISTORY_BACKEND', _DEFAULT_HISTORY_BACKEND)
        state_dir = cls._coerce_state_dir()
        match backend:
            case 'sqlite':
                return SQLiteHistoryStore(state_dir / 'history.sqlite')
            case 'jsonl':
                return JsonlHistoryStore(state_dir / 'history.jsonl')
            case _:
                raise ValueError(
                    'ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl',
                )

    # -- Instance Methods -- #

    def iter_runs(self) -> Iterator[dict[str, Any]]:
        """Yield one normalized run record per ``run_id`` from a history backend."""
        merged_by_run_id: dict[str, dict[str, Any]] = {}
        run_order: list[str] = []

        for record in self.iter_records():
            if self._record_level(record) != 'run':
                continue
            run_id = record.get('run_id')
            if not isinstance(run_id, str) or not run_id:
                continue
            if run_id not in merged_by_run_id:
                merged_by_run_id[run_id] = {}
                run_order.append(run_id)
            self._merge_record(merged_by_run_id[run_id], record)

        for run_id in run_order:
            merged = merged_by_run_id[run_id]
            yield {field: merged.get(field) for field in _RUN_RECORD_FIELDS}

    def iter_job_runs(self) -> Iterator[dict[str, Any]]:
        """Yield one normalized job-run record per ``(run_id, job_name)`` key."""
        merged_by_job_key: dict[tuple[str, str], dict[str, Any]] = {}
        job_order: list[tuple[str, str]] = []

        for record in self.iter_records():
            if self._record_level(record) != 'job':
                continue
            run_id = record.get('run_id')
            job_name = record.get('job_name')
            if not isinstance(run_id, str) or not run_id:
                continue
            if not isinstance(job_name, str) or not job_name:
                continue
            key = (run_id, job_name)
            if key not in merged_by_job_key:
                merged_by_job_key[key] = {}
                job_order.append(key)
            self._merge_record(merged_by_job_key[key], record)

        for key in job_order:
            merged = merged_by_job_key[key]
            yield {field: merged.get(field) for field in _JOB_RUN_RECORD_FIELDS}

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
    def _record_level(
        record: Mapping[str, Any],
    ) -> str:
        """Return the persisted history level for one raw record."""
        level = record.get('record_level')
        if level == 'job':
            return 'job'
        return 'run'

    @staticmethod
    def _merge_record(
        record: dict[str, Any],
        update: Mapping[str, Any],
    ) -> None:
        """Merge a partial history update into an accumulated run record."""
        for key, value in update.items():
            if value is not None or key not in record:
                record[key] = value

    # -- Abstract Instance Methods -- #

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def record_run_finished(
        self,
        completion: RunCompletion,
    ) -> None:
        """
        Persist completion or failure details for a run.

        Parameters
        ----------
        completion : RunCompletion
            Stable completion details for the run.

        Raises
        ------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def record_job_run(
        self,
        record: JobRunRecord,
    ) -> None:
        """
        Persist one completed job-run record for a DAG-style execution.

        Parameters
        ----------
        record : JobRunRecord
            Persistable job-run record to store.

        Raises
        ------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        raise NotImplementedError


# SECTION: CLASSES ========================================================== #


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
        payload = record.to_payload()
        payload['record_level'] = 'run'
        self._append_record(payload)

    def record_run_finished(
        self,
        completion: RunCompletion,
    ) -> None:
        """
        Persist completion or failure details for a run by appending a record
        to the log.

        Parameters
        ----------
        completion : RunCompletion
            Stable completion details for the run.
        """
        payload = completion.to_payload()
        payload['record_level'] = 'run'
        payload['schema_version'] = HISTORY_SCHEMA_VERSION
        self._append_record(payload)

    def record_job_run(
        self,
        record: JobRunRecord,
    ) -> None:
        """
        Persist one completed job-run record by appending it to the log.

        Parameters
        ----------
        record : JobRunRecord
            Persistable job-run record.
        """
        payload = record.to_payload()
        payload['record_level'] = 'job'
        payload['schema_version'] = HISTORY_SCHEMA_VERSION
        self._append_record(payload)


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
        with closing(self._connect()) as conn, conn:
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
                CREATE TABLE IF NOT EXISTS job_runs (
                    run_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    pipeline_name TEXT,
                    sequence_index INTEGER NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    duration_ms INTEGER,
                    records_in INTEGER,
                    records_out INTEGER,
                    status TEXT NOT NULL,
                    result_status TEXT,
                    error_type TEXT,
                    error_message TEXT,
                    skipped_due_to TEXT,
                    result_summary TEXT,
                    PRIMARY KEY (run_id, job_name)
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
        with closing(self._connect()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    run_id,
                    pipeline_name,
                    job_name,
                    NULL AS sequence_index,
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
                    NULL AS result_status,
                    NULL AS skipped_due_to,
                    host,
                    pid,
                    etlplus_version,
                    'run' AS record_level
                FROM runs
                UNION ALL
                SELECT
                    run_id,
                    pipeline_name,
                    job_name,
                    sequence_index,
                    NULL AS config_path,
                    NULL AS config_sha256,
                    status,
                    started_at,
                    finished_at,
                    duration_ms,
                    records_in,
                    records_out,
                    error_type,
                    error_message,
                    NULL AS error_traceback,
                    result_summary,
                    result_status,
                    skipped_due_to,
                    NULL AS host,
                    NULL AS pid,
                    NULL AS etlplus_version,
                    'job' AS record_level
                FROM job_runs
                ORDER BY started_at ASC, run_id ASC, sequence_index ASC, job_name ASC
                """,
            )
            for row in rows:
                yield _sqlite_row_payload(cast(sqlite3.Row, row))

    def record_run_finished(
        self,
        completion: RunCompletion,
    ) -> None:
        """
        Record completion or failure details for a run.

        Parameters
        ----------
        completion : RunCompletion
            Stable completion details for the run.
        """
        state = completion.state
        with closing(self._connect()) as conn, conn:
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
                    state.status,
                    state.finished_at,
                    state.duration_ms,
                    _serialize_result_summary(state.result_summary),
                    state.error_type,
                    state.error_message,
                    state.error_traceback,
                    completion.run_id,
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
        payload = _sqlite_record_payload(record)
        with closing(self._connect()) as conn, conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO runs (
                    {_RUN_DB_COLUMNS_SQL}
                ) VALUES ({_RUN_DB_PLACEHOLDERS})
                """,
                tuple(payload[column] for column in _RUN_DB_COLUMNS),
            )

    def record_job_run(
        self,
        record: JobRunRecord,
    ) -> None:
        """
        Persist one completed job-run record in the SQLite history store.

        Parameters
        ----------
        record : JobRunRecord
            Persistable job-run record.
        """
        payload = _sqlite_job_run_payload(record)
        with closing(self._connect()) as conn, conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO job_runs (
                    {_JOB_RUN_DB_COLUMNS_SQL}
                ) VALUES ({_JOB_RUN_DB_PLACEHOLDERS})
                """,
                tuple(payload[column] for column in _JOB_RUN_DB_COLUMNS),
            )
