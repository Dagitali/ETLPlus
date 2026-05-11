"""
:mod:`etlplus.queue._aws` module.

AWS SQS queue metadata helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import ClassVar
from typing import Self
from typing import TypedDict

from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._base import ProviderQueueConfigMixin
from ._enums import QueueService
from ._enums import QueueType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'AwsSqsQueue',
    'AwsSqsQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class AwsSqsQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`AwsSqsQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.queue.AwsSqsQueue.from_obj`
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


# SECTION: INTERNAL CONSTANTS =============================================== #


_AWS_SQS_INTEGER_RANGES: dict[str, tuple[int, int]] = {
    'delay_seconds': (0, 900),
    'max_messages': (1, 10),
    'message_retention_period': (60, 1_209_600),
    'visibility_timeout': (0, 43_200),
    'wait_time_seconds': (0, 20),
}

_AWS_SQS_FIFO_FIELDS = (
    'content_based_deduplication',
    'deduplication_id',
    'message_group_id',
)

_AWS_SQS_OPTION_FIELDS = (
    'url',
    'arn',
    'region',
    *_AWS_SQS_INTEGER_RANGES,
    'content_based_deduplication',
    'dead_letter_queue_arn',
    'deduplication_id',
    'message_group_id',
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class AwsSqsQueue(ProviderQueueConfigMixin):
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

    # -- Instance Attributes -- #

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

    # -- Internal Class Attributes -- #

    _integer_ranges: ClassVar[dict[str, tuple[int, int]]] = _AWS_SQS_INTEGER_RANGES
    _fifo_fields: ClassVar[tuple[str, ...]] = _AWS_SQS_FIFO_FIELDS
    _options_attr: ClassVar[str] = 'attributes'
    _option_fields: ClassVar[tuple[str, ...]] = _AWS_SQS_OPTION_FIELDS

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into an ``AwsSqsQueue`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed queue instance.
        """
        common_fields = cls._common_fields(
            obj,
            label='AwsSqsQueue',
            options_field='attributes',
            options_key='attributes',
        )
        name = common_fields['name']
        queue_type_value = obj.get('queue_type', obj.get('type'))
        queue_type = (
            QueueType.coerce(queue_type_value)
            if queue_type_value is not None
            else QueueType.FIFO
            if name.endswith('.fifo')
            else QueueType.STANDARD
        )
        integer_values = {
            field_name: ValueParser.optional_int(
                obj.get(field_name),
                field_name=field_name,
                label='AwsSqsQueue',
            )
            for field_name in cls._integer_ranges
        }
        content_based_deduplication = obj.get('content_based_deduplication')
        queue = cls(
            **common_fields,
            queue_type=queue_type,
            url=ValueParser.optional_str(obj.get('url')),
            arn=ValueParser.optional_str(obj.get('arn')),
            region=ValueParser.optional_str(obj.get('region')),
            **integer_values,
            content_based_deduplication=(
                None
                if content_based_deduplication is None
                else ValueParser.bool_flag(content_based_deduplication, default=False)
            ),
            dead_letter_queue_arn=ValueParser.optional_str(
                obj.get('dead_letter_queue_arn'),
            ),
            deduplication_id=ValueParser.optional_str(obj.get('deduplication_id')),
            message_group_id=ValueParser.optional_str(obj.get('message_group_id')),
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

    # -- Internal Instance Methods -- #

    def _base_connector_options(self) -> dict[str, Any]:
        """
        Return AWS SQS base connector options.

        Returns
        -------
        dict[str, Any]
            Base connector option fields.
        """
        return {
            'service': self.service.value,
            'queue_type': self.queue_type.value,
            'queue_name': self.name,
        }

    # -- Instance Methods -- #

    def validate(self) -> None:
        """
        Validate SQS naming rules captured by the queue type.

        Raises
        ------
        ValueError
            If queue metadata violates SQS naming, range, or FIFO constraints.
        """
        if self.queue_type is QueueType.FIFO and not self.name.endswith('.fifo'):
            raise ValueError('SQS FIFO queue names must end with ".fifo"')
        if self.queue_type is QueueType.STANDARD and any(
            getattr(self, field_name) is not None for field_name in self._fifo_fields
        ):
            raise ValueError(
                'SQS FIFO fields require queue_type="fifo" and a ".fifo" queue name',
            )
        for field_name, (minimum, maximum) in self._integer_ranges.items():
            value = getattr(self, field_name)
            if value is not None and not minimum <= value <= maximum:
                raise ValueError(
                    f'AwsSqsQueue "{field_name}" must be between '
                    f'{minimum} and {maximum}',
                )
