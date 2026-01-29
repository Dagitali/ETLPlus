"""
:mod:`etlplus.connector.file` module.

File connector configuration dataclass.

Notes
-----
- TypedDicts in this module are intentionally ``total=False`` and are not
    enforced at runtime.
- :meth:`*.from_obj` constructors accept :class:`Mapping[str, Any]` and perform
    tolerant parsing and light casting. This keeps the runtime permissive while
    improving autocomplete and static analysis for contributors.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Self
from typing import TypedDict
from typing import overload

from ..types import StrAnyMap
from ..utils import coerce_dict
from .core import ConnectorBase
from .enums import DataConnectorType
from .types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorFile',
    'ConnectorFileConfigMap',
]


# SECTION: TYPED DICTS ====================================================== #


class ConnectorFileConfigMap(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorFile.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.file.ConnectorFile.from_obj`
    """

    name: str
    type: ConnectorType
    format: str
    path: str
    options: StrAnyMap


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True)
class ConnectorFile(ConnectorBase):
    """
    Configuration for a file-based data connector.

    Attributes
    ----------
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
        name = cls._require_name(obj, kind='File')

        return cls(
            name=name,
            format=obj.get('format'),
            path=obj.get('path'),
            options=coerce_dict(obj.get('options')),
        )
