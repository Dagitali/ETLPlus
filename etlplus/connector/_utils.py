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


def _load_connector(
    kind: DataConnectorType,
) -> type[Connector]:
    """
    Resolve the connector class for the requested kind.

    Parameters
    ----------
    kind : DataConnectorType
        Connector kind enum.

    Returns
    -------
    type[Connector]
        Connector class corresponding to *kind*.

    Raises
    ------
    TypeError
        If *kind* is unsupported.
    """
    match kind:
        case DataConnectorType.API:
            return ConnectorApi
        case DataConnectorType.DATABASE:
            return ConnectorDb
        case DataConnectorType.FILE:
            return ConnectorFile
        case _:
            raise TypeError(f'Unsupported connector type: {kind!r}')


# SECTION: FUNCTIONS ======================================================== #


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
    connector_cls = _load_connector(_coerce_connector_type(obj))
    return connector_cls.from_obj(obj)
