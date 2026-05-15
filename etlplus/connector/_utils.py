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
from ._queue import ConnectorQueue

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
    DataConnectorType.QUEUE: ConnectorQueue,
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


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
    if 'type' not in obj:
        raise TypeError('Connector requires a "type"')

    try:
        connector_type = DataConnectorType.coerce(obj.get('type'))
    except ValueError as exc:
        allowed = ', '.join(DataConnectorType.choices())
        raise TypeError(
            f'Unsupported connector type: {obj.get("type")!r}. '
            f'Expected one of {allowed}.',
        ) from exc

    return _CONNECTOR_CLASSES[connector_type].from_obj(obj)
