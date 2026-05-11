"""
:mod:`etlplus.queue._providers` module.

Configuration metadata for non-SQS queue providers.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import ClassVar
from typing import Self
from typing import TypedDict

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AmqpQueue',
    'AmqpQueueConfigDict',
    'AzureServiceBusQueue',
    'AzureServiceBusQueueConfigDict',
    'GcpPubSubQueue',
    'GcpPubSubQueueConfigDict',
    'RedisQueue',
    'RedisQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class AzureServiceBusQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`AzureServiceBusQueue.from_obj`."""

    name: str
    namespace: str
    queue_name: str
    topic: str
    subscription: str
    options: StrAnyMap


class GcpPubSubQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`GcpPubSubQueue.from_obj`."""

    name: str
    project: str
    topic: str
    subscription: str
    options: StrAnyMap


class AmqpQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`AmqpQueue.from_obj`."""

    name: str
    url: str
    host: str
    virtual_host: str
    exchange: str
    routing_key: str
    options: StrAnyMap


class RedisQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`RedisQueue.from_obj`."""

    name: str
    url: str
    key: str
    database: int
    options: StrAnyMap


# SECTION: MIXINS =========================================================== #


class ProviderQueueConfigMixin:
    """Shared behavior for provider-specific queue config objects."""

    # -- Dunder Instance Attributes -- #

    __slots__ = ()

    # -- Instance Attributes -- #

    service: QueueService
    options: dict[str, Any]

    # --Internal Instance Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = ()

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        data = {**self.options, 'service': self.service.value}
        for field_name in self._option_fields:
            value = getattr(self, field_name)
            if value is not None:
                data[field_name] = value
        return data


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _optional_int(value: object, *, field_name: str, label: str) -> int | None:
    """
    Return one optional integer value.

    Parameters
    ----------
    value : object
        Input value.
    field_name : str
        Field name used in validation errors.
    label : str
        Human-readable payload label used in validation errors.

    Returns
    -------
    int | None
        Parsed integer value, or ``None`` when absent.

    Raises
    ------
    TypeError
        If the value cannot be parsed as an integer.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f'{label} "{field_name}" must be an integer')
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f'{label} "{field_name}" must be an integer') from exc


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class AzureServiceBusQueue(ProviderQueueConfigMixin):
    """Configuration metadata for Azure Service Bus queues/subscriptions."""

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.AZURE_SERVICE_BUS
    namespace: str | None = None
    queue_name: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = (
        'namespace',
        'queue_name',
        'topic',
        'subscription',
    )

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into an ``AzureServiceBusQueue`` instance."""
        queue = cls(
            name=MappingFieldParser.require_str(
                obj,
                'name',
                label='AzureServiceBusQueue',
            ),
            namespace=ValueParser.optional_str(obj.get('namespace')),
            queue_name=ValueParser.optional_str(
                obj.get('queue_name', obj.get('queue')),
            ),
            topic=ValueParser.optional_str(obj.get('topic')),
            subscription=ValueParser.optional_str(obj.get('subscription')),
            options=MappingParser.to_dict(obj.get('options')),
        )
        queue.validate()
        return queue

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate Azure Service Bus queue/topic metadata.

        Raises
        ------
        ValueError
            If queue metadata lacks a queue or topic target, or if a subscription
            is provided without a topic.
        """
        if self.subscription is not None and self.topic is None:
            raise ValueError(
                'AzureServiceBusQueue "subscription" requires "topic"',
            )
        if self.queue_name is None and self.topic is None:
            raise ValueError(
                'AzureServiceBusQueue requires "queue_name" or "topic"',
            )


@dataclass(kw_only=True, slots=True)
class GcpPubSubQueue(ProviderQueueConfigMixin):
    """Configuration metadata for Google Cloud Pub/Sub topics/subscriptions."""

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.GCP_PUBSUB
    project: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Internal Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = (
        'project',
        'topic',
        'subscription',
    )

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into a ``GcpPubSubQueue`` instance."""
        queue = cls(
            name=MappingFieldParser.require_str(obj, 'name', label='GcpPubSubQueue'),
            project=ValueParser.optional_str(obj.get('project')),
            topic=ValueParser.optional_str(obj.get('topic')),
            subscription=ValueParser.optional_str(obj.get('subscription')),
            options=MappingParser.to_dict(obj.get('options')),
        )
        queue.validate()
        return queue

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate Google Cloud Pub/Sub metadata.

        Raises
        ------
        ValueError
            If queue metadata lacks a project or Pub/Sub topic/subscription target.
        """
        if self.project is None:
            raise ValueError('GcpPubSubQueue requires "project"')
        if self.topic is None and self.subscription is None:
            raise ValueError('GcpPubSubQueue requires "topic" or "subscription"')


@dataclass(kw_only=True, slots=True)
class AmqpQueue(ProviderQueueConfigMixin):
    """Configuration metadata for AMQP/RabbitMQ queues."""

    name: str
    service: QueueService = QueueService.AMQP
    url: str | None = None
    host: str | None = None
    virtual_host: str | None = None
    exchange: str | None = None
    routing_key: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Internal Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = (
        'url',
        'host',
        'virtual_host',
        'exchange',
        'routing_key',
    )

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into an ``AmqpQueue`` instance."""
        queue = cls(
            name=MappingFieldParser.require_str(obj, 'name', label='AmqpQueue'),
            url=ValueParser.optional_str(obj.get('url')),
            host=ValueParser.optional_str(obj.get('host')),
            virtual_host=ValueParser.optional_str(obj.get('virtual_host')),
            exchange=ValueParser.optional_str(obj.get('exchange')),
            routing_key=ValueParser.optional_str(obj.get('routing_key')),
            options=MappingParser.to_dict(obj.get('options')),
        )
        queue.validate()
        return queue

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate AMQP connection metadata.

        Raises
        ------
        ValueError
            If queue metadata lacks a URL or host.
        """
        if self.url is None and self.host is None:
            raise ValueError('AmqpQueue requires "url" or "host"')


@dataclass(kw_only=True, slots=True)
class RedisQueue(ProviderQueueConfigMixin):
    """Configuration metadata for Redis queue-like workflows."""

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.REDIS
    url: str | None = None
    key: str | None = None
    database: int | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Internal Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = ('url', 'key', 'database')

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into a ``RedisQueue`` instance."""
        queue = cls(
            name=MappingFieldParser.require_str(obj, 'name', label='RedisQueue'),
            url=ValueParser.optional_str(obj.get('url')),
            key=ValueParser.optional_str(obj.get('key', obj.get('queue_name'))),
            database=_optional_int(
                obj.get('database', obj.get('db')),
                field_name='database',
                label='RedisQueue',
            ),
            options=MappingParser.to_dict(obj.get('options')),
        )
        queue.validate()
        return queue

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate Redis queue metadata.

        Raises
        ------
        ValueError
            If the Redis database number is negative.
        """
        if self.database is not None and self.database < 0:
            raise ValueError('RedisQueue "database" must be greater than or equal to 0')
