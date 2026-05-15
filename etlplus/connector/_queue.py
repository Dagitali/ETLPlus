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

from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self
from typing import TypedDict

from ..queue import AmqpQueue
from ..queue import AwsSqsQueue
from ..queue import AzureServiceBusQueue
from ..queue import GcpPubSubQueue
from ..queue import QueueConfig
from ..queue import QueueService
from ..queue import QueueType
from ..queue import RedisQueue
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


type QueueConfigFactory = Callable[[StrAnyMap], QueueConfig]


_QUEUE_CONFIG_FACTORIES: dict[QueueService, QueueConfigFactory] = {
    QueueService.AWS_SQS: AwsSqsQueue.from_obj,
    QueueService.AZURE_SERVICE_BUS: AzureServiceBusQueue.from_obj,
    QueueService.GCP_PUBSUB: GcpPubSubQueue.from_obj,
    QueueService.AMQP: AmqpQueue.from_obj,
    QueueService.REDIS: RedisQueue.from_obj,
}


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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorQueue(ConnectorBase):
    """
    Configuration for a queue-based data connector.

    Attributes
    ----------
    type : DataConnectorType
        Connector kind, always ``'queue'``.
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

    # -- Internal Class Methods -- #

    @classmethod
    def _queue_name_from_obj(
        cls,
        obj: StrAnyMap,
    ) -> str | None:
        """Return the first normalized queue name alias with a value."""
        for field_name in ('queue_name', 'queue'):
            if (
                queue_name := ValueParser.optional_str(obj.get(field_name))
            ) is not None:
                return queue_name
        return None

    @classmethod
    def _queue_type_from_obj(
        cls,
        obj: StrAnyMap,
        *,
        queue_name: str | None,
    ) -> QueueType:
        """Return the normalized queue type inferred from payload fields."""
        queue_type_value = obj.get('queue_type')
        return (
            QueueType.coerce(queue_type_value)
            if queue_type_value is not None
            else QueueType.FIFO
            if queue_name is not None and queue_name.endswith('.fifo')
            else QueueType.STANDARD
        )

    @classmethod
    def _service_from_obj(
        cls,
        obj: StrAnyMap,
    ) -> QueueService:
        """Return the normalized queue service, defaulting to AWS SQS."""
        service = obj.get('service')
        return (
            QueueService.AWS_SQS
            if service is None
            else QueueService.coerce(service)
        )

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

        Raises
        ------
        ValueError
            If an SQS FIFO queue name does not end with ``'.fifo'``.
        """
        queue_name = cls._queue_name_from_obj(obj)
        queue_type = cls._queue_type_from_obj(obj, queue_name=queue_name)
        if queue_type is QueueType.FIFO and (
            queue_name is not None and not queue_name.endswith('.fifo')
        ):
            raise ValueError('SQS FIFO queue names must end with ".fifo"')

        return cls(
            name=cls._name_from_obj(obj),
            service=cls._service_from_obj(obj),
            queue_type=queue_type,
            queue_name=queue_name,
            url=cls._optional_str(obj, 'url'),
            region=cls._optional_str(obj, 'region'),
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

        Raises
        ------
        ValueError
            If the queue service is unsupported.
        """
        data = {
            **self.options,
            'name': self.queue_name or self.name,
            'queue_type': self.queue_type,
        }
        for field_name in ('queue_name', 'region', 'url'):
            if (value := getattr(self, field_name)) is not None:
                data[field_name] = value
        try:
            return _QUEUE_CONFIG_FACTORIES[self.service](data)
        except KeyError as exc:
            raise ValueError(
                f'Unsupported queue service: {self.service!r}',
            ) from exc
