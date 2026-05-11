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

from ..utils._types import StrAnyMap
from ._base import ProviderQueueConfigMixin
from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AmqpQueue',
    'AmqpQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class AmqpQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`AmqpQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.queue.AmqpQueue.from_obj`
    """

    name: str
    url: str
    host: str
    virtual_host: str
    exchange: str
    routing_key: str
    options: StrAnyMap


# SECTION: INTERNAL CONSTANTS =============================================== #


_AMQP_OPTION_FIELDS = (
    'url',
    'host',
    'virtual_host',
    'exchange',
    'routing_key',
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class AmqpQueue(ProviderQueueConfigMixin):
    """
    Configuration metadata for AMQP/RabbitMQ queues.

    Attributes
    ----------
    service : QueueService
        Queue service, always ``'amqp'``.
    name : str
        Queue metadata name.
    url : str | None
        Optional AMQP connection URL.
    host : str | None
        Optional AMQP host.
    virtual_host : str | None
        Optional AMQP virtual host.
    exchange : str | None
        Optional AMQP exchange.
    routing_key : str | None
        Optional AMQP routing key.
    options : dict[str, Any]
        Optional provider-specific queue options.
    """

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.AMQP
    url: str | None = None
    host: str | None = None
    virtual_host: str | None = None
    exchange: str | None = None
    routing_key: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Internal Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = _AMQP_OPTION_FIELDS

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into an ``AmqpQueue`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed queue instance.
        """
        queue = cls(
            **cls._common_fields(obj, label='AmqpQueue'),
            **cls._optional_str_fields(
                obj,
                'url',
                'host',
                'virtual_host',
                'exchange',
                'routing_key',
            ),
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
