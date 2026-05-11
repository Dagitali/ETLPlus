"""
:mod:`etlplus.queue._gcp` module.

Google Cloud Pub/Sub queue metadata helpers.
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
    'GcpPubSubQueue',
    'GcpPubSubQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class GcpPubSubQueueConfigDict(TypedDict, total=False):
    """Shape accepted by :meth:`GcpPubSubQueue.from_obj`."""

    name: str
    project: str
    topic: str
    subscription: str
    options: StrAnyMap


# SECTION: DATA CLASSES ===================================================== #


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
