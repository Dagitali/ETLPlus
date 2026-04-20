"""
:mod:`etlplus.cli._handlers._history_view` module.

History-query helpers shared by CLI history handlers.
"""

from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from typing import Literal
from typing import cast

from ...history import HistoryStore
from ...utils import JsonCodec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'HISTORY_TABLE_COLUMNS',
    'JOB_HISTORY_TABLE_COLUMNS',
    # Classes
    'HistoryView',
]


# SECTION: TYPE ALIASES ===================================================== #


type HistoryLevel = Literal['run', 'job']
type HistoryRecord = Mapping[str, Any]


# SECTION: CONSTANTS ======================================================== #


HISTORY_TABLE_COLUMNS = (
    'run_id',
    'status',
    'job_name',
    'pipeline_name',
    'started_at',
    'finished_at',
    'duration_ms',
)

JOB_HISTORY_TABLE_COLUMNS = (
    'run_id',
    'sequence_index',
    'job_name',
    'status',
    'result_status',
    'pipeline_name',
    'started_at',
    'finished_at',
    'duration_ms',
)

_TABLE_COLUMNS_BY_LEVEL: dict[HistoryLevel, tuple[str, ...]] = {
    'job': JOB_HISTORY_TABLE_COLUMNS,
    'run': HISTORY_TABLE_COLUMNS,
}


# SECTION: CLASSES ========================================================== #


class HistoryView:
    """Shared query helpers for persisted history records."""

    # -- Static Methods -- #

    @staticmethod
    def fingerprint(
        record: HistoryRecord,
    ) -> str:
        """
        Return a stable fingerprint for a persisted history record.

        The fingerprint is used to identify related start and finish events
        that should be merged into one run record for CLI output. The
        fingerprint is not guaranteed to be unique across all records, but it
        should be stable for related events of the same run and different
        enough across unrelated runs to avoid collisions in practice.
        """
        return JsonCodec.serialize(record, sort_keys=True)

    @staticmethod
    def parse_timestamp(
        value: object,
    ) -> datetime | None:
        """
        Parse an ISO-8601 timestamp used in persisted history records.

        The parser should be lenient to accommodate different timestamp
        formats. The expected format is a subset of ISO-8601 that includes date
        and time components, with optional timezone information. The parser
        should also handle timestamps that end with 'Z' to indicate UTC time,
        as well as timestamps that include timezone offsets. If the input value
        is not a valid timestamp string, the function should return ``None``.

        Examples of valid timestamp formats include:
        - "2023-01-01T12:00:00Z"
        - "2023-01-01T12:00:00+00:00"
        - "2023-01-01T12:00:00-05:00"
        - "2023-01-01T12:00:00"

        Parameters
        ----------
        value : object
            The value to parse as a timestamp.

        Returns
        -------
        datetime | None
            A datetime object if parsing is successful, or ``None`` if the
            input value is not a valid timestamp string.
        """
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def record_timestamp(
        record: HistoryRecord,
    ) -> datetime | None:
        """
        Return the normalized timestamp used for record filtering.

        The timestamp is derived from the record's :attr:`started_at` or
        :attr:`finished_at` fields, whichever is available. If neither field
        contains a valid timestamp, the function returns ``None``. This allows
        for consistent filtering of history records based on their relevant
        timestamps, even if the original record formats vary.

        Parameters
        ----------
        record : HistoryRecord
            The history record from which to extract the timestamp.

        Returns
        -------
        datetime | None
            A datetime object if a valid timestamp is found, otherwise ``None``.
        """
        return HistoryView.parse_timestamp(
            record.get('started_at') or record.get('finished_at'),
        )

    @staticmethod
    def sort_key(
        record: HistoryRecord,
    ) -> tuple[str, str, int, str]:
        """
        Return a reverse-sortable key for history records.

        The key is a tuple of (timestamp, run_id), where the timestamp is
        derived from the record's :attr:`started_at` or :attr:`finished_at`
        fields, and the run_id is derived from the record's :attr:`run_id`
        field. This allows for consistent sorting of history records in reverse
        chronological order.

        Parameters
        ----------
        record : HistoryRecord
            The history record from which to extract the sort key.

        Returns
        -------
        tuple[str, str, int, str]
            A tuple containing the timestamp, run_id, sequence index, and job
            name for sorting.
        """
        timestamp = cast(
            str,
            record.get('started_at') or record.get('finished_at') or '',
        )
        run_id = cast(str, record.get('run_id') or '')
        sequence_index = (
            int(record['sequence_index'])
            if isinstance(record.get('sequence_index'), int)
            else -1
        )
        job_name = cast(str, record.get('job_name') or '')
        return (timestamp, run_id, sequence_index, job_name)

    @staticmethod
    def table_columns(
        level: HistoryLevel,
    ) -> tuple[str, ...]:
        """
        Return the Markdown table columns for one history level.

        The columns are determined by the history level, which can be either
        'run' or 'job'. The function looks up the appropriate column set from a
        predefined mapping and returns it as a tuple of strings. This allows
        for consistent formatting of CLI output based on the type of history
        records being displayed.

        Parameters
        ----------
        level : HistoryLevel
            The history level for which to retrieve the table columns. Valid
            values are 'run' and 'job'.

        Returns
        -------
        tuple[str, ...]
            A tuple of column names to be used for Markdown table output.
        """
        return _TABLE_COLUMNS_BY_LEVEL[level]

    @staticmethod
    def validate_output_mode(
        *,
        json_output: bool,
        table: bool,
    ) -> None:
        """
        Validate that at most one explicit history output mode was requested.

        The CLI supports multiple output modes for history commands, such as
        JSON and table formats. This function checks that the user did not
        request more than one explicit output mode at the same time, which
        would be ambiguous. If multiple modes are requested, the function
        raises a :class:`ValueError` to prompt the user to choose only one.

        Parameters
        ----------
        json_output : bool
            Whether JSON output mode was requested.
        table : bool
            Whether table output mode was requested.

        Raises
        ------
        ValueError
            If more than one explicit output mode was requested.
        """
        if json_output and table:
            raise ValueError('choose either json output or table output, not both')

    # -- Internal Class Methods -- #

    @classmethod
    def _matching_records(
        cls,
        *,
        raw: bool,
        level: HistoryLevel,
        job: str | None,
        pipeline: str | None,
        run_id: str | None,
        since: datetime | None,
        until: datetime | None,
        status: str | None,
    ) -> list[dict[str, Any]]:
        """
        Return materialized history records that match the given filters.

        Parameters
        ----------
        raw : bool
            Whether to load raw history records.
        level : HistoryLevel
            Whether to load run-level or job-level records.
        job : str | None, optional
            Filter records by job name.
        pipeline : str | None, optional
            Filter records by pipeline name.
        run_id : str | None, optional
            Filter records by run ID.
        since : datetime | None, optional
            Filter records starting from this timestamp.
        until : datetime | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter records by status.

        Returns
        -------
        list[dict[str, Any]]
            A list of filtered history records.
        """
        history_store = HistoryStore.from_environment()
        matching: list[dict[str, Any]] = []
        for record in cls._records_iter(
            history_store,
            raw=raw,
            level=level,
        ):
            payload = dict(record)
            payload.setdefault('record_level', level if not raw else 'run')
            if not cls.matches(
                payload,
                level=level,
                job=job,
                pipeline=pipeline,
                run_id=run_id,
                since=since,
                until=until,
                status=status,
            ):
                continue
            matching.append(
                {key: value for key, value in payload.items() if key != 'record_level'},
            )
        return matching

    @staticmethod
    def _records_iter(
        history_store: HistoryStore,
        *,
        raw: bool,
        level: HistoryLevel,
    ) -> Iterator[dict[str, Any]]:
        """Return the appropriate record iterator for the requested view mode."""
        if raw:
            return history_store.iter_records()
        if level == 'job':
            return history_store.iter_job_runs()
        return history_store.iter_runs()

    # -- Class Methods -- #

    @classmethod
    def load_records(
        cls,
        *,
        raw: bool,
        level: HistoryLevel = 'run',
        job: str | None = None,
        limit: int | None = None,
        pipeline: str | None = None,
        run_id: str | None = None,
        since: str | None = None,
        until: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Load, filter, and sort history records for CLI read commands.

        Parameters
        ----------
        raw : bool
            Whether to load raw history records.
        level : HistoryLevel, optional
            Whether to load run-level or job-level history entries.
        job : str | None, optional
            Filter records by job name.
        limit : int | None, optional
            Limit the number of records returned.
        pipeline : str | None, optional
            Filter records by pipeline name.
        run_id : str | None, optional
            Filter records by run ID.
        since : str | None, optional
            Filter records starting from this timestamp.
        until : str | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter records by status.

        Returns
        -------
        list[dict[str, Any]]
            A list of filtered and sorted history records.
        """
        records = sorted(
            cls._matching_records(
                raw=raw,
                level=level,
                job=job,
                pipeline=pipeline,
                run_id=run_id,
                since=cls.parse_timestamp(since),
                until=cls.parse_timestamp(until),
                status=status,
            ),
            key=cls.sort_key,
            reverse=True,
        )
        if limit is None:
            return records
        return records[:limit]

    @classmethod
    def matches(
        cls,
        record: HistoryRecord,
        *,
        level: HistoryLevel = 'run',
        job: str | None = None,
        pipeline: str | None = None,
        run_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        status: str | None = None,
    ) -> bool:
        """
        Return whether a history record matches CLI filter values.

        Parameters
        ----------
        record : HistoryRecord
            The history record to check.
        level : HistoryLevel, optional
            Filter by run-level or job-level record scope.
        job : str | None, optional
            Filter by job name.
        pipeline : str | None, optional
            Filter by pipeline name.
        run_id : str | None, optional
            Filter by run ID.
        since : datetime | None, optional
            Filter records starting from this timestamp.
        until : datetime | None, optional
            Filter records up to this timestamp.
        status : str | None, optional
            Filter by status.

        Returns
        -------
        bool
            True if the record matches the filters, False otherwise.
        """
        record_level = cast(str, record.get('record_level') or 'run')
        if record_level != level:
            return False
        if job is not None and record.get('job_name') != job:
            return False
        if pipeline is not None and record.get('pipeline_name') != pipeline:
            return False
        if run_id is not None and record.get('run_id') != run_id:
            return False
        if status is not None and record.get('status') != status:
            return False
        record_timestamp = cls.record_timestamp(record)
        if since is not None:
            if record_timestamp is None or record_timestamp < since:
                return False
        if until is not None:
            if record_timestamp is None or record_timestamp > until:
                return False
        return True
