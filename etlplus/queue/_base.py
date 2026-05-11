"""
:mod:`etlplus.queue._base` module.

Shared queue configuration protocols.
"""

from __future__ import annotations

from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import runtime_checkable

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils._types import StrAnyMap
from ._enums import QueueService

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Mixins
    'ProviderQueueConfigMixin',
    # Protocols
    'QueueConfigProtocol',
]


# SECTION: MIXINS =========================================================== #


class ProviderQueueConfigMixin:
    """Shared behavior for provider-specific queue config objects."""

    # -- Dunder Instance Attributes -- #

    __slots__ = ()

    # -- Instance Attributes -- #

    service: QueueService
    options: dict[str, Any]

    # -- Internal Instance Attributes -- #

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

    # -- Internal Static Methods -- #

    @staticmethod
    def _common_fields(
        obj: StrAnyMap,
        *,
        label: str,
        options_field: str = 'options',
        options_key: str = 'options',
    ) -> dict[str, Any]:
        """
        Return shared provider queue config constructor fields.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name`` and optional ``options``.
        label : str
            Human-readable payload label used in validation errors.
        options_field : str, optional
            Constructor field name that receives parsed provider options.
        options_key : str, optional
            Input mapping key that contains provider options.

        Returns
        -------
        dict[str, Any]
            Parsed ``name`` and provider-specific options fields.

        Raises
        ------
        TypeError
            If ``name`` is missing or invalid.
        """
        return {
            'name': MappingFieldParser.require_str(obj, 'name', label=label),
            options_field: MappingParser.to_dict(obj.get(options_key)),
        }


# SECTION: PROTOCOLS ======================================================== #


@runtime_checkable
class QueueConfigProtocol(Protocol):
    """Structural contract for provider-specific queue configuration objects."""

    # -- Attributes -- #

    name: str
    service: QueueService

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        ...
