"""
:mod:`etlplus.queue._redis` module.

Redis queue metadata helpers.
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
    'RedisQueue',
    'RedisQueueConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class RedisQueueConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`RedisQueue.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.queue.RedisQueue.from_obj`
    """

    name: str
    url: str
    key: str
    database: int
    options: StrAnyMap


# SECTION: INTERNAL CONSTANTS =============================================== #


_REDIS_OPTION_FIELDS = ('url', 'key', 'database')


# SECTION: DATA CLASSES ===================================================== #


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

    _option_fields: ClassVar[tuple[str, ...]] = _REDIS_OPTION_FIELDS

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``RedisQueue`` instance.

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
            name=MappingFieldParser.require_str(obj, 'name', label='RedisQueue'),
            url=ValueParser.optional_str(obj.get('url')),
            key=ValueParser.optional_str(obj.get('key', obj.get('queue_name'))),
            database=ValueParser.optional_int(
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
