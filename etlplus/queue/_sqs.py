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
    delay_seconds: int
    max_messages: int
    message_retention_period: int
    visibility_timeout: int
    wait_time_seconds: int
    content_based_deduplication: bool
    dead_letter_queue_arn: str
    deduplication_id: str
    message_group_id: str
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


def _optional_bool(
    value: object,
) -> bool | None:
    """Return one optional boolean flag when a value is present."""
    if value is None:
        return None
    return ValueParser.bool_flag(value, default=False)


def _optional_int(
    value: object,
    *,
    field_name: str,
) -> int | None:
    """
    Return one optional integer value.

    Parameters
    ----------
    value : object
        Input value.
    field_name : str
        Field name used in validation errors.

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
        raise TypeError(f'SqsQueue "{field_name}" must be an integer')
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f'SqsQueue "{field_name}" must be an integer') from exc


def _validate_range(
    value: int | None,
    *,
    field_name: str,
    minimum: int,
    maximum: int,
) -> None:
    """
    Validate one optional integer field range.

    Parameters
    ----------
    value : int | None
        Parsed integer value.
    field_name : str
        Field name used in validation errors.
    minimum : int
        Inclusive minimum value.
    maximum : int
        Inclusive maximum value.

    Raises
    ------
    ValueError
        If *value* falls outside the inclusive range.
    """
    if value is None:
        return
    if value < minimum or value > maximum:
        raise ValueError(
            f'SqsQueue "{field_name}" must be between {minimum} and {maximum}',
        )


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class SqsQueue:
    """
    Configuration metadata for an AWS SQS queue.

    Attributes
    ----------
    service : QueueService
        Queue service, always ``'aws-sqs'``.
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
    delay_seconds : int | None
        Optional default message delay in seconds.
    max_messages : int | None
        Optional receive batch size hint.
    message_retention_period : int | None
        Optional message retention period in seconds.
    visibility_timeout : int | None
        Optional visibility timeout in seconds.
    wait_time_seconds : int | None
        Optional long-poll wait time in seconds.
    content_based_deduplication : bool | None
        Optional FIFO queue content-based deduplication flag.
    dead_letter_queue_arn : str | None
        Optional dead-letter queue ARN.
    deduplication_id : str | None
        Optional FIFO message deduplication ID hint.
    message_group_id : str | None
        Optional FIFO message group ID hint.
    attributes : dict[str, Any]
        Optional SQS queue attributes.
    """

    # -- Attributes -- #

    name: str
    service: QueueService = QueueService.AWS_SQS
    queue_type: QueueType = QueueType.STANDARD
    url: str | None = None
    arn: str | None = None
    region: str | None = None
    delay_seconds: int | None = None
    max_messages: int | None = None
    message_retention_period: int | None = None
    visibility_timeout: int | None = None
    wait_time_seconds: int | None = None
    content_based_deduplication: bool | None = None
    dead_letter_queue_arn: str | None = None
    deduplication_id: str | None = None
    message_group_id: str | None = None
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
            delay_seconds=_optional_int(
                obj.get('delay_seconds'),
                field_name='delay_seconds',
            ),
            max_messages=_optional_int(
                obj.get('max_messages'),
                field_name='max_messages',
            ),
            message_retention_period=_optional_int(
                obj.get('message_retention_period'),
                field_name='message_retention_period',
            ),
            visibility_timeout=_optional_int(
                obj.get('visibility_timeout'),
                field_name='visibility_timeout',
            ),
            wait_time_seconds=_optional_int(
                obj.get('wait_time_seconds'),
                field_name='wait_time_seconds',
            ),
            content_based_deduplication=_optional_bool(
                obj.get('content_based_deduplication'),
            ),
            dead_letter_queue_arn=ValueParser.optional_str(
                obj.get('dead_letter_queue_arn'),
            ),
            deduplication_id=ValueParser.optional_str(obj.get('deduplication_id')),
            message_group_id=ValueParser.optional_str(obj.get('message_group_id')),
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
        for field_name in (
            'delay_seconds',
            'max_messages',
            'message_retention_period',
            'visibility_timeout',
            'wait_time_seconds',
            'content_based_deduplication',
            'dead_letter_queue_arn',
            'deduplication_id',
            'message_group_id',
        ):
            value = getattr(self, field_name)
            if value is not None:
                data[field_name] = value
        return data

    def validate(self) -> None:
        """
        Validate SQS naming rules captured by the queue type.

        Raises
        ------
        ValueError
            If queue metadata violates SQS naming, range, or FIFO constraints.
        """
        if self.is_fifo and not self.name.endswith('.fifo'):
            raise ValueError('SQS FIFO queue names must end with ".fifo"')
        if self.is_standard and (
            self.content_based_deduplication is not None
            or self.deduplication_id is not None
            or self.message_group_id is not None
        ):
            raise ValueError(
                'SQS FIFO fields require queue_type="fifo" and a ".fifo" queue name',
            )
        _validate_range(
            self.delay_seconds,
            field_name='delay_seconds',
            minimum=0,
            maximum=900,
        )
        _validate_range(
            self.max_messages,
            field_name='max_messages',
            minimum=1,
            maximum=10,
        )
        _validate_range(
            self.message_retention_period,
            field_name='message_retention_period',
            minimum=60,
            maximum=1_209_600,
        )
        _validate_range(
            self.visibility_timeout,
            field_name='visibility_timeout',
            minimum=0,
            maximum=43_200,
        )
        _validate_range(
            self.wait_time_seconds,
            field_name='wait_time_seconds',
            minimum=0,
            maximum=20,
        )


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
