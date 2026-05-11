"""
:mod:`etlplus.queue._amqp` module.

AMQP/RabbitMQ queue metadata helpers.
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
from ._providers import ProviderQueueConfigMixin

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AmqpQueue',
    'AmqpQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class AmqpQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`AmqpQueue.from_obj`."""

    name: str
    url: str
    host: str
    virtual_host: str
    exchange: str
    routing_key: str
    options: StrAnyMap


# SECTION: DATA CLASSES ===================================================== #


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
