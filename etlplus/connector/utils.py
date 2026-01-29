"""
:mod:`etlplus.connector.utils` module.

Shared connector parsing helpers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any

from ..types import StrAnyMap
from .core import ConnectorBase
from .enums import DataConnectorType

if TYPE_CHECKING:  # Editor-only typing hints to avoid runtime imports
    from .api import ConnectorApi
    from .database import ConnectorDb
    from .file import ConnectorFile

    type Connector = ConnectorApi | ConnectorDb | ConnectorFile

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'parse_connector',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_connector_type(
    obj: Mapping[str, Any],
) -> DataConnectorType:
    """
    Normalize and validate the connector ``type`` field.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with a ``type`` entry.

    Returns
    -------
    DataConnectorType
        Normalized connector type enum.

    Raises
    ------
    TypeError
        If ``type`` is missing or unsupported.
    """
    if 'type' not in obj:
        raise TypeError('Connector requires a "type"')
    try:
        return DataConnectorType.coerce(obj.get('type'))
    except ValueError as exc:
        allowed = ', '.join(DataConnectorType.choices())
        raise TypeError(
            f'Unsupported connector type: {obj.get("type")!r}. '
            f'Expected one of {allowed}.',
        ) from exc


def _require_name(
    obj: StrAnyMap,
    *,
    kind: str,
) -> str:
    """
    Extract and validate the ``name`` field from connector mappings.

    Parameters
    ----------
    obj : StrAnyMap
        Connector mapping with a ``name`` entry.
    kind : str
        Connector kind used in the error message.

    Returns
    -------
    str
        Valid connector name.

    Raises
    ------
    TypeError
        If ``name`` is missing or not a string.
    """
    name = obj.get('name')
    if not isinstance(name, str):
        raise TypeError(f'Connector{kind} requires a "name" (str)')
    return name


# SECTION: FUNCTIONS ======================================================== #


def parse_connector(
    obj: Mapping[str, Any],
) -> ConnectorBase:
    """
    Dispatch to a concrete connector constructor based on ``type``.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with at least ``name`` and ``type``.

    Returns
    -------
    ConnectorBase
        Concrete connector instance.

    Raises
    ------
    TypeError
        If the mapping is invalid or the connector type is unsupported.

    Notes
    -----
    Delegates to the tolerant ``from_obj`` constructors for each connector
    kind. Connector types are normalized via
    :class:`etlplus.connector.enums.DataConnectorType`, so common aliases
    (e.g., ``'db'`` or ``'http'``) are accepted.
    """
    if not isinstance(obj, Mapping):
        raise TypeError('Connector configuration must be a mapping.')
    match _coerce_connector_type(obj):
        case DataConnectorType.FILE:
            from .file import ConnectorFile

            return ConnectorFile.from_obj(obj)
        case DataConnectorType.DATABASE:
            from .database import ConnectorDb

            return ConnectorDb.from_obj(obj)
        case DataConnectorType.API:
            from .api import ConnectorApi

            return ConnectorApi.from_obj(obj)
