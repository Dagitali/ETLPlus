"""
:mod:`etlplus.queue._location` module.

Parsed queue-location helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote
from urllib.parse import urlsplit

from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'QueueLocation',
]


# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True, slots=True)
class QueueLocation:
    """
    Parsed queue location.

    Attributes
    ----------
    raw : str
        Original location string.
    service : QueueService
        Normalized queue service.
    path : str
        Queue path/name within the service.
    authority : str
        Optional service authority such as region, namespace, or host.
    """

    # -- Attributes -- #

    raw: str
    service: QueueService
    path: str
    authority: str = ''

    # -- Class Methods -- #

    @classmethod
    def from_value(
        cls,
        value: str,
    ) -> QueueLocation:
        """
        Parse a queue URI into a :class:`QueueLocation`.

        Parameters
        ----------
        value : str
            Queue URI such as ``aws-sqs://us-east-1/events.fifo``.

        Returns
        -------
        QueueLocation
            Parsed queue location.

        Raises
        ------
        ValueError
            If *value* is empty, lacks a service scheme, or lacks a path.
        """
        raw = str(value).strip()
        if not raw:
            raise ValueError('Queue location cannot be empty')

        parsed = urlsplit(raw)
        if not parsed.scheme:
            raise ValueError('Queue location requires a service scheme')

        service = QueueService.coerce(parsed.scheme)
        path = unquote(parsed.path.lstrip('/'))
        if not path:
            raise ValueError('Queue location requires a queue path')
        return cls(
            raw=raw,
            service=service,
            authority=parsed.netloc,
            path=path,
        )
