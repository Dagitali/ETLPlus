"""
:mod:`etlplus.workflow._errors` module.

Exception types with structured context for workflow and DAG failures.

Summary
-------
Provides workflow-specific errors for invalid dependency graphs and related
orchestration validation failures.

Examples
--------
>>> try:
...     raise DagError(message="Dependency cycle detected")
... except DagError as error:
...     print(error.message)
Dependency cycle detected
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Errors
    'DagError',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True, kw_only=True)
class DagError(ValueError):
    """
    Error raised when a workflow dependency graph is invalid.

    Parameters
    ----------
    message : str
        Human-readable error message describing the graph failure.

    Attributes
    ----------
    message : str
        Human-readable error message describing the graph failure.

    Notes
    -----
    The :meth:`as_dict` helper returns a structured payload suitable for
    logging or telemetry.
    """

    # -- Attributes -- #

    message: str

    # -- Magic Methods (Object Representation) -- #

    def __str__(self) -> str:  # pragma: no cover - formatting only
        return self.message

    # -- Instance Methods -- #

    def as_dict(self) -> dict[str, Any]:
        """Return structured workflow error context."""
        return {'message': self.message}
