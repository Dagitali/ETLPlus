"""
:mod:`etlplus.connector._core` module.

Protocols and base classes for connector implementations.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol
from typing import Self
from typing import runtime_checkable

from ..utils import MappingFieldParser
from ..utils import MappingParser
from ..utils import ValueParser
from ..utils._types import StrAnyMap
from ._enums import DataConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Abstract Base Classes
    'ConnectorBase',
    # Protocols
    'ConnectorProtocol',
]


# SECTION: PROTOCOLS ======================================================== #


@runtime_checkable
class ConnectorProtocol(Protocol):
    """
    Structural contract for connector implementations.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : DataConnectorType
        Connector kind.
    """

    # -- Attributes -- #

    name: str
    type: DataConnectorType

    # -- Class Methods -- #

    @classmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """
        Parse a mapping into a connector instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.

        Raises
        ------
        NotImplementedError
            Protocol placeholder. Concrete connector classes provide the
            implementation.
        """
        raise NotImplementedError


# SECTION: ABSTRACT BASE DATA CLASSES ======================================= #


@dataclass(kw_only=True, slots=True)
class ConnectorBase(ABC, ConnectorProtocol):
    """
    Abstract base class for connector implementations.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : DataConnectorType
        Connector kind.
    """

    name: str
    type: DataConnectorType

    # -- Internal Class Methods -- #

    @classmethod
    def _dict_field(
        cls,
        obj: StrAnyMap,
        field_name: str,
    ) -> dict[str, object]:
        """Return one mapping-like field as a plain ``dict``."""
        return MappingParser.to_dict(obj.get(field_name))

    @classmethod
    def _name_from_obj(cls, obj: StrAnyMap) -> str:
        """Return the required connector name for this connector class."""
        return MappingFieldParser.require_str(obj, 'name', label=cls.__name__)

    @classmethod
    def _optional_str(
        cls,
        obj: StrAnyMap,
        *field_names: str,
    ) -> str | None:
        """Return the first optional string field present in *obj*."""
        for field_name in field_names:
            if field_name in obj:
                return ValueParser.optional_str(obj.get(field_name))
        return None

    @classmethod
    def _str_dict_field(
        cls,
        obj: StrAnyMap,
        field_name: str,
    ) -> dict[str, str]:
        """Return one mapping-like field as a plain ``dict[str, str]``."""
        return MappingParser.to_str_dict(MappingParser.optional(obj.get(field_name)))

    # -- Class Methods -- #

    @classmethod
    @abstractmethod
    def from_obj(cls, obj: StrAnyMap) -> Self:
        """
        Parse a mapping into a connector instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.
        """
