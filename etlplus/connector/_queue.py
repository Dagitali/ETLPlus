"""
:mod:`etlplus.connector._queue` module.

Queue connector configuration dataclass.

Notes
-----
- TypedDicts in this module are intentionally ``total=False`` and are not
    enforced at runtime.
- :meth:`*.from_obj` constructors accept :class:`Mapping[str, Any]` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self
from typing import TypedDict

from ..queue import AmqpQueue
from ..queue import AzureServiceBusQueue
from ..queue import GcpPubSubQueue
from ..queue import QueueConfig
from ..queue import QueueService
from ..queue import QueueType
from ..queue import RedisQueue
from ..queue import SqsQueue
from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._core import ConnectorBase
from ._enums import DataConnectorType
from ._types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorQueue',
    'ConnectorQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class ConnectorQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.ConnectorQueue.from_obj`
    """

    name: str
    type: ConnectorType
    service: QueueService | str
    queue_type: QueueType | str
    queue_name: str
    url: str
    region: str
    options: StrAnyMap


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_queue_type(
    *,
    queue_name: str | None,
    value: object,
) -> QueueType:
    """
    Coerce an explicit queue type or infer FIFO from the queue name.

    Parameters
    ----------
    queue_name : str | None
        Optional queue name.
    value : object
        Optional explicit queue type value.

    Returns
    -------
    QueueType
        Normalized queue type.
    """
    if value is not None:
        return QueueType.coerce(value)
    return (
        QueueType.FIFO
        if queue_name is not None and queue_name.endswith('.fifo')
        else QueueType.STANDARD
    )


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorQueue(ConnectorBase):
    """
    Configuration for a queue-based data connector.

    Attributes
    ----------
    type : DataConnectorType
        Connector kind (always ``'queue'``).
    service : QueueService
        Queue service provider (e.g., ``'aws-sqs'``).
    queue_type : QueueType
        Queue ordering/delivery type: ``'standard'`` or ``'fifo'``.
    queue_name : str | None
        Queue name.
    url : str | None
        Queue URL.
    region : str | None
        Queue region.
    options : dict[str, Any]
        Service-specific queue options.
    """

    # -- Attributes -- #

    type: DataConnectorType = DataConnectorType.QUEUE
    service: QueueService = QueueService.AWS_SQS
    queue_type: QueueType = QueueType.STANDARD
    queue_name: str | None = None
    url: str | None = None
    region: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorQueue`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.
        """
        name = MappingFieldParser.require_str(obj, 'name', label='ConnectorQueue')
        queue_name = ValueParser.optional_str(obj.get('queue_name', obj.get('queue')))
        queue_type = _coerce_queue_type(
            queue_name=queue_name,
            value=obj.get('queue_type'),
        )
        if queue_type is QueueType.FIFO and (
            queue_name is not None and not queue_name.endswith('.fifo')
        ):
            raise ValueError('SQS FIFO queue names must end with ".fifo"')

        return cls(
            name=name,
            service=QueueService.coerce(obj.get('service', QueueService.AWS_SQS)),
            queue_type=queue_type,
            queue_name=queue_name,
            url=ValueParser.optional_str(obj.get('url')),
            region=ValueParser.optional_str(obj.get('region')),
            options=MappingParser.to_dict(obj.get('options')),
        )

    # -- Instance Methods -- #

    def to_queue_config(self) -> QueueConfig:
        """
        Convert this connector into a provider-specific queue config.

        Returns
        -------
        QueueConfig
            Provider-specific queue configuration object.
        """
        data = {
            **self.options,
            'name': self.queue_name or self.name,
            'queue_type': self.queue_type,
        }
        for field_name in ('queue_name', 'region', 'url'):
            if (value := getattr(self, field_name)) is not None:
                data[field_name] = value
        match self.service:
            case QueueService.AWS_SQS:
                return SqsQueue.from_obj(data)
            case QueueService.AZURE_SERVICE_BUS:
                return AzureServiceBusQueue.from_obj(data)
            case QueueService.GCP_PUBSUB:
                return GcpPubSubQueue.from_obj(data)
            case QueueService.AMQP:
                return AmqpQueue.from_obj(data)
            case QueueService.REDIS:
                return RedisQueue.from_obj(data)
