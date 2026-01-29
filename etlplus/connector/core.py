"""
:mod:`etlplus.connector.core` module.

Protocols for connector implementations.
"""

from __future__ import annotations

from typing import Protocol
from typing import Self
from typing import runtime_checkable

from ..types import StrAnyMap
from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
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
