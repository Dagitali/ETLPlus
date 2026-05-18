"""
:mod:`etlplus.connector._diagnostics` module.

Shared connector diagnostic wording and remediation policy.
"""

from __future__ import annotations

from ..storage import StorageScheme
from ..utils import TextNormalizer
from ._database import ConnectorDb
from ._enums import DataConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ConnectorDiagnosticPolicy',
]


# SECTION: INTERNAL CONSTANTS =============================================== #

_GUIDANCE = {
    'missing path': (
        'Set "path" to a local path or storage URI for this file connector.'
    ),
    'missing url or api reference': (
        'Set "url" to a reachable endpoint or "api" to a configured '
        'top-level API name.'
    ),
    'missing connection_string': (
        'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
    ),
}


# SECTION: CLASSES ========================================================== #


class ConnectorDiagnosticPolicy:
    """Centralized wording for connector config diagnostics."""

    # -- Static Methods -- #

    @staticmethod
    def connector_type_choices() -> tuple[str, ...]:
        """
        Return the supported connector type names.

        This is used for diagnostics and should not be confused with the set of
        supported storage schemes, which are not connector types but may be
        used in connector paths or URI schemes.

        Returns
        -------
        tuple[str, ...]
            Supported connector type names.
        """
        return DataConnectorType.choices()

    @staticmethod
    def connector_type_guidance(
        connector_type: str,
    ) -> str:
        """
        Return actionable guidance for an unsupported connector type.

        Parameters
        ----------
        connector_type : str
            The unsupported connector type to provide guidance for.

        Returns
        -------
        str
            Guidance message for the unsupported connector type.
        """
        supported = ', '.join(ConnectorDiagnosticPolicy.connector_type_choices())
        normalized = TextNormalizer.normalize(connector_type)
        if not normalized:
            return f'Set type to one of: {supported}.'
        try:
            StorageScheme.coerce(normalized)
        except ValueError:
            return f'Use one of the supported connector types: {supported}.'
        return (
            f'"{normalized}" is a storage scheme, not a connector type. '
            'Use connector type "file" and keep the provider in the path '
            'or URI scheme.'
        )

    @staticmethod
    def gap_guidance(
        *,
        api_reference: str | None = None,
        issue: str,
    ) -> str | None:
        """
        Return one remediation string for a connector config gap.

        Parameters
        ----------
        api_reference : str | None
            The API reference associated with the gap, if any.
        issue : str
            The specific issue to provide guidance for.

        Returns
        -------
        str | None
            Guidance message for the specified connector config gap, or
            ``None`` if no guidance is available.
        """
        _GUIDANCE.get(issue)
        if _GUIDANCE is not None:
            return _GUIDANCE

        match issue:
            case 'missing connection_string or bigquery project/dataset':
                return ConnectorDb.provider_missing_connection_guidance('bigquery')
            case 'missing connection_string or snowflake account/database/schema':
                return ConnectorDb.provider_missing_connection_guidance('snowflake')
            case issue_text if issue_text.startswith('unknown api reference: '):
                if api_reference:
                    return (
                        f'Define "{api_reference}" under top-level "apis" or '
                        'update the connector "api" reference.'
                    )
                return 'Define the referenced API under top-level "apis".'
            case _:
                return None

    @staticmethod
    def invalid_entry_guidance() -> str:
        """Return guidance for invalid connector entry shapes."""
        return (
            'Define each connector as a mapping with at least "name" and "type" '
            'fields.'
        )

    @staticmethod
    def missing_name_guidance() -> str:
        """Return guidance for missing or blank connector names."""
        return 'Set "name" to a non-empty string.'
