"""
:mod:`etlplus.connector._file` module.

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

from ..utils import MappingParser
from ..utils._types import StrAnyMap
from ._core import ConnectorBase
from ._enums import DataConnectorType
from ._types import ConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ConnectorFile',
    'ConnectorFileConfigDict',
]


# SECTION: TYPED DICTS ====================================================== #


class ConnectorFileConfigDict(TypedDict, total=False):
    """
    Shape accepted by :meth:`ConnectorFile.from_obj` (all keys optional).

    See Also
    --------
    - :meth:`etlplus.connector.ConnectorFile.from_obj`
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
    type : DataConnectorType
        Connector kind, always ``'file'``.
    format : str | None
        File format (e.g., ``'json'``, ``'csv'``).
    path : str | None
        File path or URI.
    options : dict[str, Any]
        Reader/writer format options.
    """

    # -- Attributes -- #

    type: DataConnectorType = DataConnectorType.FILE
    format: str | None = None
    path: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    # -- Class Methods -- #

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
        return cls(
            name=cls._name_from_obj(obj),
            format=cls._optional_str(obj, 'format'),
            path=cls._optional_str(obj, 'path'),
            options=MappingParser.to_dict(obj.get('options')),
        )
