"""
:mod:`etlplus.queue._providers` module.

Configuration metadata for non-SQS queue providers.
"""

from __future__ import annotations

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


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class AzureServiceBusQueue:
    """Configuration metadata for Azure Service Bus queues/subscriptions."""

    name: str
    service: QueueService = QueueService.AZURE_SERVICE_BUS
    namespace: str | None = None
    queue_name: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into an ``AzureServiceBusQueue`` instance."""
        name = MappingFieldParser.require_str(
            obj,
            'name',
            label='AzureServiceBusQueue',
        )
        return cls(
            name=name,
            namespace=ValueParser.optional_str(obj.get('namespace')),
            queue_name=ValueParser.optional_str(
                obj.get('queue_name', obj.get('queue')),
            ),
            topic=ValueParser.optional_str(obj.get('topic')),
            subscription=ValueParser.optional_str(obj.get('subscription')),
            options=MappingParser.to_dict(obj.get('options')),
        )

    def to_connector_options(self) -> dict[str, Any]:
        """Return a connector-friendly options mapping."""
        return _options_with_fields(
            self.options,
            service=self.service.value,
            namespace=self.namespace,
            queue_name=self.queue_name,
            topic=self.topic,
            subscription=self.subscription,
        )


@dataclass(kw_only=True, slots=True)
class GcpPubSubQueue:
    """Configuration metadata for Google Cloud Pub/Sub topics/subscriptions."""

    name: str
    service: QueueService = QueueService.GCP_PUBSUB
    project: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into a ``GcpPubSubQueue`` instance."""
        name = MappingFieldParser.require_str(obj, 'name', label='GcpPubSubQueue')
        return cls(
            name=name,
            project=ValueParser.optional_str(obj.get('project')),
            topic=ValueParser.optional_str(obj.get('topic')),
            subscription=ValueParser.optional_str(obj.get('subscription')),
            options=MappingParser.to_dict(obj.get('options')),
        )

    def to_connector_options(self) -> dict[str, Any]:
        """Return a connector-friendly options mapping."""
        return _options_with_fields(
            self.options,
            service=self.service.value,
            project=self.project,
            topic=self.topic,
            subscription=self.subscription,
        )


@dataclass(kw_only=True, slots=True)
class AmqpQueue:
    """Configuration metadata for AMQP/RabbitMQ queues."""

    name: str
    service: QueueService = QueueService.AMQP
    url: str | None = None
    host: str | None = None
    virtual_host: str | None = None
    exchange: str | None = None
    routing_key: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into an ``AmqpQueue`` instance."""
        name = MappingFieldParser.require_str(obj, 'name', label='AmqpQueue')
        return cls(
            name=name,
            url=ValueParser.optional_str(obj.get('url')),
            host=ValueParser.optional_str(obj.get('host')),
            virtual_host=ValueParser.optional_str(obj.get('virtual_host')),
            exchange=ValueParser.optional_str(obj.get('exchange')),
            routing_key=ValueParser.optional_str(obj.get('routing_key')),
            options=MappingParser.to_dict(obj.get('options')),
        )

    def to_connector_options(self) -> dict[str, Any]:
        """Return a connector-friendly options mapping."""
        return _options_with_fields(
            self.options,
            service=self.service.value,
            url=self.url,
            host=self.host,
            virtual_host=self.virtual_host,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )


@dataclass(kw_only=True, slots=True)
class RedisQueue:
    """Configuration metadata for Redis queue-like workflows."""

    name: str
    service: QueueService = QueueService.REDIS
    url: str | None = None
    key: str | None = None
    database: int | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """Parse a mapping into a ``RedisQueue`` instance."""
        name = MappingFieldParser.require_str(obj, 'name', label='RedisQueue')
        database = obj.get('database', obj.get('db'))
        return cls(
            name=name,
            url=ValueParser.optional_str(obj.get('url')),
            key=ValueParser.optional_str(obj.get('key', obj.get('queue_name'))),
            database=None if database is None else int(database),
            options=MappingParser.to_dict(obj.get('options')),
        )

    def to_connector_options(self) -> dict[str, Any]:
        """Return a connector-friendly options mapping."""
        return _options_with_fields(
            self.options,
            service=self.service.value,
            url=self.url,
            key=self.key,
            database=self.database,
        )


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _options_with_fields(
    options: dict[str, Any],
    **fields: Any,
) -> dict[str, Any]:
    """Return options merged with non-empty metadata fields."""
    data = dict(options)
    for key, value in fields.items():
        if value is not None:
            data[key] = value
    return data
