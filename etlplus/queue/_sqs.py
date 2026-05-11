"""
:mod:`etlplus.queue._sqs` module.

AWS SQS queue type helpers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self
from typing import TypedDict

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._enums import QueueService
from ._enums import QueueType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SqsQueue',
    'SqsQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class SqsQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`SqsQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.queue.SqsQueue.from_obj`
    """

    name: str
    queue_type: QueueType | str
    url: str
    arn: str
    region: str
    attributes: StrAnyMap


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _infer_queue_type(
    *,
    name: str,
    value: object,
) -> QueueType:
    """
    Coerce an explicit queue type or infer FIFO from an SQS name suffix.

    Parameters
    ----------
    name : str
        SQS queue name.
    value : object
        Optional explicit queue type value.

    Returns
    -------
    QueueType
        Normalized queue type.
    """
    if value is not None:
        return QueueType.coerce(value)
    return QueueType.FIFO if name.endswith('.fifo') else QueueType.STANDARD


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class SqsQueue:
    """
    Configuration metadata for an AWS SQS queue.

    Attributes
    ----------
    service : QueueService
        Queue service, always ``'sqs'``.
    name : str
        SQS queue name.
    queue_type : QueueType
        Queue type, either ``'standard'`` or ``'fifo'``.
    url : str | None
        Optional queue URL.
    arn : str | None
        Optional queue ARN.
    region : str | None
        Optional AWS region.
    attributes : dict[str, Any]
        Optional SQS queue attributes.
    """

    # -- Attributes -- #

    name: str
    service: QueueService = QueueService.SQS
    queue_type: QueueType = QueueType.STANDARD
    url: str | None = None
    arn: str | None = None
    region: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into an ``SqsQueue`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed queue instance.
        """
        name = MappingFieldParser.require_str(obj, 'name', label='SqsQueue')
        queue_type = _infer_queue_type(
            name=name,
            value=obj.get('queue_type', obj.get('type')),
        )
        queue = cls(
            name=name,
            queue_type=queue_type,
            url=ValueParser.optional_str(obj.get('url')),
            arn=ValueParser.optional_str(obj.get('arn')),
            region=ValueParser.optional_str(obj.get('region')),
            attributes=MappingParser.to_dict(obj.get('attributes')),
        )
        queue.validate()
        return queue

    # -- Getters -- #

    @property
    def is_fifo(self) -> bool:
        """Return whether this queue uses SQS FIFO semantics."""
        return self.queue_type is QueueType.FIFO

    @property
    def is_standard(self) -> bool:
        """Return whether this queue uses standard SQS semantics."""
        return self.queue_type is QueueType.STANDARD

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping for this queue.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        data: dict[str, Any] = dict(self.attributes)
        data.update(
            {
                'service': self.service.value,
                'queue_type': self.queue_type.value,
                'queue_name': self.name,
            },
        )
        if self.url is not None:
            data['url'] = self.url
        if self.arn is not None:
            data['arn'] = self.arn
        if self.region is not None:
            data['region'] = self.region
        return data

    def validate(self) -> None:
        """
        Validate SQS naming rules captured by the queue type.

        Raises
        ------
        ValueError
            If a FIFO queue name does not end with ``.fifo``.
        """
        if self.is_fifo and not self.name.endswith('.fifo'):
            raise ValueError('SQS FIFO queue names must end with ".fifo"')


def from_mapping(
    obj: Mapping[str, Any],
) -> SqsQueue:
    """
    Build an SQS queue from any mapping-like configuration.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with at least ``name``.

    Returns
    -------
    SqsQueue
        Parsed queue instance.
    """
    return SqsQueue.from_obj(dict(obj))
