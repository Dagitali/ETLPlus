"""
:mod:`etlplus.queue._base` module.

Shared queue configuration protocols.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import ClassVar
from typing import Protocol
from typing import runtime_checkable

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
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

    # -- Internal Instance Attributes -- #

    _options_attr: ClassVar[str] = 'options'
    _option_fields: ClassVar[tuple[str, ...]] = ()

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
        """
        return {
            'name': MappingFieldParser.require_str(obj, 'name', label=label),
            options_field: MappingParser.to_dict(obj.get(options_key)),
        }

    @staticmethod
    def _optional_str_fields(
        obj: StrAnyMap,
        *field_names: str,
        aliases: Mapping[str, str] | None = None,
    ) -> dict[str, str | None]:
        """
        Return parsed optional string constructor fields.

        Parameters
        ----------
        obj : StrAnyMap
            Input mapping with optional string-like fields.
        *field_names : str
            Constructor and preferred input field names to parse.
        aliases : Mapping[str, str] | None, optional
            Mapping of constructor field names to fallback input field names.

        Returns
        -------
        dict[str, str | None]
            Parsed optional string fields keyed by constructor field name.
        """
        aliases = aliases or {}
        return {
            field_name: ValueParser.optional_str(
                obj.get(field_name, obj.get(aliases.get(field_name, field_name))),
            )
            for field_name in field_names
        }

    # -- Internal Instance Methods -- #

    def _base_connector_options(self) -> dict[str, Any]:
        """
        Return provider base connector options.

        Returns
        -------
        dict[str, Any]
            Base connector option fields.
        """
        return {'service': self.service.value}

    # -- Instance Methods -- #

    def to_connector_options(self) -> dict[str, Any]:
        """
        Return a connector-friendly options mapping.

        Returns
        -------
        dict[str, Any]
            Queue metadata represented as a plain dictionary.
        """
        data: dict[str, Any] = dict(getattr(self, self._options_attr))
        data.update(self._base_connector_options())
        for field_name in self._option_fields:
            value = getattr(self, field_name)
            if value is not None:
                data[field_name] = value
        return data


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
