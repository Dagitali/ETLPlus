"""
:mod:`etlplus.connector._utils` module.

Shared connector parsing helpers.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._api import ConnectorApi
from ._connector import Connector
from ._database import ConnectorDb
from ._enums import DataConnectorType
from ._file import ConnectorFile

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'parse_connector',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


_CONNECTOR_CLASSES: dict[DataConnectorType, type[Connector]] = {
    DataConnectorType.API: ConnectorApi,
    DataConnectorType.DATABASE: ConnectorDb,
    DataConnectorType.FILE: ConnectorFile,
}


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


def parse_connector(
    obj: Mapping[str, Any],
) -> Connector:
    """
    Dispatch to a concrete connector constructor based on ``type``.

    Parameters
    ----------
    obj : Mapping[str, Any]
        Mapping with at least ``name`` and ``type``.

    Returns
    -------
    Connector
        Concrete connector instance.

    Raises
    ------
    TypeError
        If the mapping is invalid or the connector type is unsupported.

    Notes
    -----
    Delegates to the tolerant ``from_obj`` constructors for each connector
    kind. Connector types are normalized via
    :class:`etlplus.connector.DataConnectorType`, so common aliases
    (e.g., ``'db'`` or ``'http'``) are accepted.
    """
    if not isinstance(obj, Mapping):
        raise TypeError('Connector configuration must be a mapping.')
    return _CONNECTOR_CLASSES[_coerce_connector_type(obj)].from_obj(obj)
