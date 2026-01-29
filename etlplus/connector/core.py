"""
:mod:`etlplus.connector.core` module.

Protocols and base classes for connector implementations.
"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol
from typing import Self
from typing import runtime_checkable

from ..types import StrAnyMap
from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorBase',
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
    type : ConnectorType
        Connector kind.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType

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


# SECTION: ABSTRACT BASE DATA CLASSES ======================================= #


@dataclass(kw_only=True, slots=True)
class ConnectorBase(ABC, ConnectorProtocol):
    """
    Abstract base class for connector implementations.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind.
    """

    name: str
    type: ConnectorType

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
