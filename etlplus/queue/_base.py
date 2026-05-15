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
    def _optional_str(
        obj: StrAnyMap,
        field_name: str,
        *,
        alias: str | None = None,
    ) -> str | None:
        """
        Return one parsed optional string field.

        Parameters
        ----------
        obj : StrAnyMap
            Input mapping with optional string-like fields.
        field_name : str
            Preferred input field name to parse.
        alias : str | None, optional
            Fallback input field name when *field_name* is absent.

        Returns
        -------
        str | None
            Parsed optional string field.
        """
        if field_name in obj:
            return ValueParser.optional_str(obj.get(field_name))
        if alias is not None and alias in obj:
            return ValueParser.optional_str(obj.get(alias))
        return None

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

        Raises
        ------
        NotImplementedError
            Protocol placeholder. Concrete queue config classes provide the
            implementation.
        """
        raise NotImplementedError
