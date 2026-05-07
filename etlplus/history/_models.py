"""
:mod:`etlplus.history._models` module.

Shared run-history record models and builders.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from dataclasses import fields
from typing import Any
from typing import Self

from ..__version__ import __version__
from ..utils import PathHasher
from ..utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'JobRunRecord',
    'RunCompletion',
    'RunState',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _field_payload(
    obj: Any,
    *,
    exclude: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Return one dataclass-backed payload mapping for *obj*."""
    return {
        field.name: getattr(obj, field.name)
        for field in fields(obj)
        if field.name not in exclude
    }


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
        return _field_payload(self)


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
            config_sha256=PathHasher.sha256(config_path),
            started_at=started_at,
            records_in=None,
            records_out=None,
            state=RunState.running(status=status),
            host=socket.gethostname(),
            pid=os.getpid(),
            etlplus_version=__version__,
        )

    # -- Instance Methods -- #

    def to_payload(self) -> dict[str, Any]:
        """Return the flat persisted representation of the run record."""
        return (
            _field_payload(self, exclude=frozenset({'state'})) | self.state.to_payload()
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
        return _field_payload(self)
