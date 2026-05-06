"""
:mod:`etlplus.history._store` module.

Local run-history persistence backends.
"""

from __future__ import annotations

import json
import os
import sqlite3
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import closing
from contextlib import contextmanager
from dataclasses import fields
from pathlib import Path
from typing import Any

from ..file.ndjson import NdjsonFile
from ..file.sqlite import SqliteFile
from ..utils import JsonCodec
from ..utils._types import JSONData
from ._config import DEFAULT_HISTORY_BACKEND
from ._config import HistoryBackend
from ._config import ResolvedHistoryConfig
from ._config import _coerce_backend
from ._config import _coerce_state_dir
from ._models import JobRunRecord
from ._models import RunCompletion
from ._models import RunRecord
from ._models import RunState
from ._models import _file_sha256 as _models_file_sha256

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HistoryStore',
    'JsonlHistoryStore',
    'SQLiteHistoryStore',
    # Constants
    'HISTORY_SCHEMA_VERSION',
    # Data Classes
    'JobRunRecord',
    'RunCompletion',
    'RunRecord',
    'RunState',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_JOB_RUN_RECORD_FIELDS = tuple(field.name for field in fields(JobRunRecord))

_RUN_STATE_FIELDS = tuple(field.name for field in fields(RunState))
_RUN_RECORD_FIELDS = (
    *(field.name for field in fields(RunRecord) if field.name != 'state'),
    *_RUN_STATE_FIELDS,
)

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


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _non_empty_text(
    value: object,
) -> str | None:
    """Return one non-empty string value or ``None``."""
    return value if isinstance(value, str) and value else None


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
    if not isinstance(values := json.loads(payload), list):
        return None
    return [value for value in values if isinstance(value, str)]


def _file_sha256(
    file_path: str,
) -> str | None:
    """Return the SHA-256 digest for *file_path* when the file exists."""
    return _models_file_sha256(file_path)


def _job_run_record_key(
    record: Mapping[str, Any],
) -> tuple[str, str] | None:
    """Return the merge key for one persisted job-level record."""
    if (run_id := _non_empty_text(record.get('run_id'))) is None:
        return None
    if (job_name := _non_empty_text(record.get('job_name'))) is None:
        return None
    return (run_id, job_name)


def _run_record_key(
    record: Mapping[str, Any],
) -> str | None:
    """Return the merge key for one persisted run-level record."""
    return _non_empty_text(record.get('run_id'))


def _serialize_result_summary(
    result_summary: JSONData | None,
) -> str | None:
    """Serialize one optional JSON result summary for persistence."""
    if result_summary is None:
        return None
    return JsonCodec().serialize(result_summary)


def _serialize_string_list(
    values: list[str] | None,
) -> str | None:
    """Serialize one optional string list for persistence."""
    return None if values is None else JsonCodec().serialize(values)


def _sqlite_job_run_payload(
    record: JobRunRecord,
) -> dict[str, Any]:
    """Return one SQLite-ready payload for a persisted job-run record."""
    return _transform_fields(
        record.to_payload(),
        {
            'result_summary': _serialize_result_summary,
            'skipped_due_to': _serialize_string_list,
        },
    )


def _sqlite_record_payload(
    record: RunRecord,
) -> dict[str, Any]:
    """Return one SQLite-ready payload for a started run record."""
    return _transform_fields(
        record.to_payload(),
        {
            'result_summary': _serialize_result_summary,
        },
    )


def _sqlite_row_payload(
    row: sqlite3.Row,
) -> dict[str, Any]:
    """Return one SQLite row decoded into a history payload."""
    return _transform_fields(
        dict(row),
        {
            'result_summary': _deserialize_result_summary,
            'skipped_due_to': _deserialize_string_list,
        },
    )


def _transform_fields(
    payload: Mapping[str, Any],
    transformers: Mapping[str, Callable[[Any], Any]],
) -> dict[str, Any]:
    """Return one payload with selected fields transformed."""
    transformed = dict(payload)
    for field_name, transform in transformers.items():
        if field_name in transformed:
            transformed[field_name] = transform(transformed.get(field_name))
    return transformed


def _with_record_metadata(
    payload: Mapping[str, Any],
    *,
    record_level: str,
    schema_version: int | None = None,
) -> dict[str, Any]:
    """Return one persisted payload with history metadata fields applied."""
    metadata: dict[str, Any] = {'record_level': record_level}
    if schema_version is not None:
        metadata['schema_version'] = schema_version
    return dict(payload) | metadata


# SECTION: ABSTRACT BASE CLASSES ============================================ #


class HistoryStore(ABC):
    """Minimal local history-store interface."""

    # -- Class Methods -- #

    @classmethod
    def from_environment(cls) -> HistoryStore:
        """Open the configured local history backend from environment values."""
        raw_backend = os.getenv('ETLPLUS_HISTORY_BACKEND', DEFAULT_HISTORY_BACKEND)
        if _coerce_backend(raw_backend) is None:
            raise ValueError('ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl')
        return cls._from_resolved_settings(
            ResolvedHistoryConfig.resolve(None, env=os.environ),
        )

    @classmethod
    def from_settings(
        cls,
        *,
        backend: HistoryBackend | str = DEFAULT_HISTORY_BACKEND,
        state_dir: str | os.PathLike[str] | None = None,
    ) -> HistoryStore:
        """Open one supported local history backend from explicit settings."""
        if _coerce_backend(backend) is None:
            raise ValueError('ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl')
        return cls._from_resolved_settings(
            ResolvedHistoryConfig.resolve(
                None,
                backend=backend,
                state_dir=state_dir,
            ),
        )

    @classmethod
    def _from_resolved_settings(
        cls,
        settings: ResolvedHistoryConfig,
    ) -> HistoryStore:
        """Open one supported local history backend from resolved settings."""
        match settings.backend:
            case 'sqlite':
                return SQLiteHistoryStore(settings.state_dir / 'history.sqlite')
            case 'jsonl':
                return JsonlHistoryStore(settings.state_dir / 'history.jsonl')
            case _:
                raise ValueError(
                    'ETLPLUS_HISTORY_BACKEND must be one of: sqlite, jsonl',
                )

    # -- Internal Instance Methods -- #

    def _iter_merged_records(
        self,
        *,
        record_level: str,
        key_fn: Callable[[Mapping[str, Any]], Hashable | None],
        field_names: tuple[str, ...],
    ) -> Iterator[dict[str, Any]]:
        """Yield merged history records for one persisted record level."""
        merged_by_key: dict[Hashable, dict[str, Any]] = {}
        key_order: list[Hashable] = []

        for record in self.iter_records():
            if self._record_level(record) != record_level:
                continue
            if (key := key_fn(record)) is None:
                continue
            if key not in merged_by_key:
                merged_by_key[key] = {}
                key_order.append(key)
            self._merge_record(merged_by_key[key], record)

        for key in key_order:
            merged = merged_by_key[key]
            yield {field: merged.get(field) for field in field_names}

    # -- Instance Methods -- #

    def iter_job_runs(self) -> Iterator[dict[str, Any]]:
        """Yield one normalized job-run record per ``(run_id, job_name)`` key."""
        yield from self._iter_merged_records(
            record_level='job',
            key_fn=_job_run_record_key,
            field_names=_JOB_RUN_RECORD_FIELDS,
        )

    def iter_runs(self) -> Iterator[dict[str, Any]]:
        """Yield one normalized run record per ``run_id`` from a history backend."""
        yield from self._iter_merged_records(
            record_level='run',
            key_fn=_run_record_key,
            field_names=_RUN_RECORD_FIELDS,
        )

    # -- Static Methods -- #

    @staticmethod
    def _coerce_state_dir(
        state_dir: str | os.PathLike[str] | None = None,
    ) -> Path:
        """Coerce a state directory path from a value or environment variable."""
        return _coerce_state_dir(state_dir or os.getenv('ETLPLUS_STATE_DIR'))

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

    def _append_history_record(
        self,
        payload: Mapping[str, Any],
        *,
        record_level: str,
        include_schema_version: bool = False,
    ) -> None:
        """Append one history payload with backend metadata applied."""
        self._append_record(
            _with_record_metadata(
                payload,
                record_level=record_level,
                schema_version=(
                    HISTORY_SCHEMA_VERSION if include_schema_version else None
                ),
            ),
        )

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
        self._append_history_record(
            record.to_payload(),
            record_level='run',
        )

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
        self._append_history_record(
            completion.to_payload(),
            record_level='run',
            include_schema_version=True,
        )

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
        self._append_history_record(
            record.to_payload(),
            record_level='job',
            include_schema_version=True,
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
        with self._transaction() as conn:
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

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        """Yield one transactional SQLite connection."""
        with closing(self._connect()) as conn:
            with conn:
                yield conn

    # -- Instance Methods -- #

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Yield persisted SQLite run rows as dictionaries."""
        with closing(self._connect()) as conn:
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
                yield _sqlite_row_payload(row)

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
        with self._transaction() as conn:
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
        with self._transaction() as conn:
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
        with self._transaction() as conn:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO job_runs (
                    {_JOB_RUN_DB_COLUMNS_SQL}
                ) VALUES ({_JOB_RUN_DB_PLACEHOLDERS})
                """,
                tuple(payload[column] for column in _JOB_RUN_DB_COLUMNS),
            )
