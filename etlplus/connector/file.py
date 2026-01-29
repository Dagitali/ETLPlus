"""
:mod:`etlplus.connector.file` module.

File connector configuration dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import overload

from ..types import StrAnyMap
from ..utils import coerce_dict
from .core import ConnectorProtocol
from .enums import DataConnectorType
from .utils import _require_name

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .types import ConnectorFileConfigMap
    from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorFile',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorFile(ConnectorProtocol):
    """
    Configuration for a file-based data connector.

    Attributes
    ----------
    name : str
        Unique connector name.
    type : ConnectorType
        Connector kind, always ``'file'``.
    format : str | None
        File format (e.g., ``'json'``, ``'csv'``).
    path : str | None
        File path or URI.
    options : dict[str, Any]
        Reader/writer format options.
    """

    # -- Attributes -- #

    name: str
    type: ConnectorType = DataConnectorType.FILE
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

    @classmethod
    @overload
    def from_obj(cls, obj: ConnectorFileConfigMap) -> Self: ...

    @classmethod
    @overload
    def from_obj(cls, obj: StrAnyMap) -> Self: ...

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap,
    ) -> Self:
        """
        Parse a mapping into a ``ConnectorFile`` instance.

        Parameters
        ----------
        obj : StrAnyMap
            Mapping with at least ``name``.

        Returns
        -------
        Self
            Parsed connector instance.
        """
        name = _require_name(obj, kind='File')

        return cls(
            name=name,
            type=DataConnectorType.FILE,
            format=obj.get('format'),
            path=obj.get('path'),
            options=coerce_dict(obj.get('options')),
        )
