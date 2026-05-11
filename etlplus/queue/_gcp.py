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

from ..utils._types import StrAnyMap
from ._base import ProviderQueueConfigMixin
from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'GcpPubSubQueue',
    'GcpPubSubQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class GcpPubSubQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`GcpPubSubQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.queue.GcpPubSubQueue.from_obj`
    """

    name: str
    project: str
    topic: str
    subscription: str
    options: StrAnyMap


# SECTION: INTERNAL CONSTANTS =============================================== #


_GCP_PUBSUB_OPTION_FIELDS = (
    'project',
    'topic',
    'subscription',
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class GcpPubSubQueue(ProviderQueueConfigMixin):
    """
    Configuration metadata for Google Cloud Pub/Sub topics/subscriptions.

    Attributes
    ----------
    service : QueueService
        Queue service, always ``'gcp-pubsub'``.
    name : str
        Queue metadata name.
    project : str | None
        Optional Google Cloud project ID.
    topic : str | None
        Optional Pub/Sub topic.
    subscription : str | None
        Optional Pub/Sub subscription.
    options : dict[str, Any]
        Optional provider-specific queue options.
    """

    # -- Instance Attributes -- #

    name: str
    service: QueueService = QueueService.GCP_PUBSUB
    project: str | None = None
    topic: str | None = None
    subscription: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Internal Class Attributes -- #

    _option_fields: ClassVar[tuple[str, ...]] = _GCP_PUBSUB_OPTION_FIELDS

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``GcpPubSubQueue`` instance.

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
            **cls._common_fields(obj, label='GcpPubSubQueue'),
            **cls._optional_str_fields(obj, 'project', 'topic', 'subscription'),
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
