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
        """
        ...


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
    def _name_from_obj(cls, obj: StrAnyMap) -> str:
        """Return the required connector name for this connector class."""
        return MappingFieldParser.require_str(obj, 'name', label=cls.__name__)

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
