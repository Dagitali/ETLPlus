"""
:mod:`etlplus.runtime.readiness._connectors` module.

Connector and optional-dependency readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..._config import Config
from ...connector import DataConnectorType
from ...queue import QueueService
from ...utils import TextNormalizer
from ._base import ReadinessSupportPolicy
from ._support import FORMAT_EXTRA_REQUIREMENTS
from ._support import QUEUE_SERVICE_EXTRA_REQUIREMENTS
from ._support import SCHEME_EXTRA_REQUIREMENTS
from ._support import ReadinessRow
from ._support import RequirementSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ConnectorReadinessPolicy',
    # Functions
    'connector_type_choices',
    'connector_type_guidance',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _connector_gap_row(
    *,
    connector: str,
    issue: str,
    role: str,
    connector_type_str: str,
    api_reference: str | None = None,
    supported_types: tuple[str, ...] | None = None,
) -> ReadinessRow:
    """Return one normalized connector-gap row."""
    row: ReadinessRow = {
        'connector': connector,
        'issue': issue,
        'role': role,
        'type': connector_type_str,
    }
    if issue == 'unsupported type':
        row['guidance'] = connector_type_guidance(connector_type_str)
    else:
        row['guidance'] = ReadinessSupportPolicy.connector_gap_guidance(
            api_reference=api_reference,
            issue=issue,
        )
    if supported_types is not None:
        row['supported_types'] = list(supported_types)
    return row


def _connector_type(
    connector_type_str: str,
) -> DataConnectorType | None:
    """Return one coerced connector type or ``None`` when unsupported."""
    try:
        return DataConnectorType.coerce(connector_type_str)
    except ValueError:
        return None


# SECTION: FUNCTIONS ======================================================== #


def connector_type_choices() -> tuple[str, ...]:
    """
    Return the supported connector type names.

    Returns
    -------
    tuple[str, ...]
        A tuple of supported connector type names.
    """
    return tuple(str(member.value) for member in DataConnectorType)


def connector_type_guidance(
    connector_type_str: str,
) -> str:
    """
    Return actionable guidance for an unsupported connector type.

    Parameters
    ----------
    connector_type_str : str
        The connector type string to provide guidance for.

    Returns
    -------
    str
        Actionable guidance for the unsupported connector type.
    """
    supported = ', '.join(connector_type_choices())
    normalized = TextNormalizer.normalize(connector_type_str)
    if not normalized:
        return f'Set type to one of: {supported}.'
    if ReadinessSupportPolicy.coerce_connector_storage_scheme(normalized) is not None:
        return (
            f'"{normalized}" is a storage scheme, not a connector type. '
            'Use connector type "file" and keep the provider in the path '
            'or URI scheme.'
        )
    return f'Use one of the supported connector types: {supported}.'


# SECTION: INTERNAL CLASSES ================================================= #


class ConnectorReadinessPolicy:
    """Evaluate connector configuration and optional dependency readiness."""

    # -- Class Methods -- #

    @classmethod
    def gap_rows(
        cls,
        cfg: Config,
    ) -> list[ReadinessRow]:
        """
        Return connector configuration gaps that block execution.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.

        Returns
        -------
        list[ReadinessRow]
            A list of dictionaries representing the configuration gaps.
        """
        gaps: list[ReadinessRow] = []
        supported_types = connector_type_choices()
        for role, connector in ReadinessSupportPolicy.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            connector_type_name = str(getattr(connector, 'type', ''))
            coerced_type = _connector_type(connector_type_name)

            if coerced_type is None:
                gaps.append(
                    _connector_gap_row(
                        connector=connector_name,
                        connector_type_str=connector_type_name,
                        issue='unsupported type',
                        role=role,
                        supported_types=supported_types,
                    ),
                )
                continue

            match coerced_type:
                case DataConnectorType.FILE if not getattr(connector, 'path', None):
                    gaps.append(
                        _connector_gap_row(
                            connector=connector_name,
                            connector_type_str=connector_type_name,
                            issue='missing path',
                            role=role,
                        ),
                    )
                case DataConnectorType.API:
                    api_reference_value = getattr(connector, 'api', None)
                    api_reference = (
                        api_reference_value
                        if isinstance(api_reference_value, str)
                        else None
                    )
                    if not getattr(connector, 'url', None) and not api_reference:
                        gaps.append(
                            _connector_gap_row(
                                connector=connector_name,
                                connector_type_str=connector_type_name,
                                issue='missing url or api reference',
                                role=role,
                            ),
                        )
                    elif api_reference and api_reference not in cfg.apis:
                        gaps.append(
                            _connector_gap_row(
                                api_reference=api_reference,
                                connector=connector_name,
                                connector_type_str=connector_type_name,
                                issue=f'unknown api reference: {api_reference}',
                                role=role,
                            ),
                        )
                case DataConnectorType.DATABASE if not getattr(
                    connector,
                    'connection_string',
                    None,
                ):
                    gaps.append(
                        _connector_gap_row(
                            connector=connector_name,
                            connector_type_str=connector_type_name,
                            issue='missing connection_string',
                            role=role,
                        ),
                    )
                case _:
                    continue

        return gaps

    @classmethod
    def missing_requirement_rows(
        cls,
        *,
        cfg: Config,
        package_available: Callable[[str], bool],
    ) -> list[ReadinessRow]:
        """
        Return missing optional dependency rows for configured connectors.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.
        package_available : Callable[[str], bool]
            A function that returns whether a package is available.

        Returns
        -------
        list[ReadinessRow]
            A list of dictionaries representing the missing optional
            dependencies.
        """
        rows: list[ReadinessRow] = []
        for role, connector in ReadinessSupportPolicy.iter_connectors(cfg):
            connector_name = str(getattr(connector, 'name', '<unnamed>'))
            connector_type_name = str(getattr(connector, 'type', '') or '')
            path = getattr(connector, 'path', None)
            format_name = str(getattr(connector, 'format', '') or '').lower()
            queue_service_raw = str(getattr(connector, 'service', '') or '').lower()
            # TODO: Consider supporting other connector-specific fields that
            # TODO: may indicate optional dependencies, e.g. database driver
            # TODO: hints.
            queue_service = (
                coerced_service.value
                if (coerced_service := QueueService.try_coerce(queue_service_raw))
                else queue_service_raw
            )

            if path:
                scheme = ReadinessSupportPolicy.coerce_storage_scheme(path)
                requirement = SCHEME_EXTRA_REQUIREMENTS.get(scheme or '')
                if (
                    scheme
                    and requirement
                    and not requirement.is_available(
                        availability_checker=package_available,
                    )
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=connector_name,
                            detected_scheme=scheme,
                            reason=(
                                f'{scheme} storage path requires {requirement.package}'
                            ),
                            requirement=requirement,
                            role=role,
                        ),
                    )

            if (
                connector_type_name == DataConnectorType.QUEUE.value
                and queue_service in QUEUE_SERVICE_EXTRA_REQUIREMENTS
            ):
                requirement = QUEUE_SERVICE_EXTRA_REQUIREMENTS[queue_service]
                if not requirement.is_available(
                    availability_checker=package_available,
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=connector_name,
                            detected_queue_service=queue_service,
                            reason=(
                                f'{queue_service} queue connector requires '
                                f'{requirement.package}'
                            ),
                            requirement=requirement,
                            role=role,
                        ),
                    )

            if format_name == 'nc':
                if not cls.netcdf_available(
                    package_available=package_available,
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=connector_name,
                            detected_format='nc',
                            reason=(
                                'nc format requires xarray and netCDF4 or h5netcdf'
                            ),
                            requirement=RequirementSpec(
                                modules=('xarray', 'netCDF4', 'h5netcdf'),
                                package='xarray/netCDF4',
                                extra='file',
                            ),
                            role=role,
                        ),
                    )
                continue

            requirement = FORMAT_EXTRA_REQUIREMENTS.get(format_name)
            if requirement and not requirement.is_available(
                availability_checker=package_available,
            ):
                rows.append(
                    cls.requirement_row(
                        connector=connector_name,
                        detected_format=format_name,
                        reason=f'{format_name} format requires {requirement.package}',
                        requirement=requirement,
                        role=role,
                    ),
                )

        return ReadinessSupportPolicy.dedupe_rows(rows)

    @classmethod
    def readiness_checks(
        cls,
        cfg: Config,
        *,
        connector_gap_rows_fn: Callable[[Config], list[ReadinessRow]],
        make_check: Callable[..., dict[str, Any]],
        package_available: Callable[[str], bool],
    ) -> list[ReadinessRow]:
        """
        Return connector configuration and dependency readiness checks.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.
        connector_gap_rows_fn : Callable[[Config], list[ReadinessRow]]
            A function that returns connector configuration gaps.
        make_check : Callable[..., dict[str, Any]]
            A function that creates a readiness check dictionary.
        package_available : Callable[[str], bool]
            A function that returns whether a package is available.

        Returns
        -------
        list[ReadinessRow]
            A list of dictionaries representing the readiness checks.
        """
        checks: list[ReadinessRow] = []
        gaps = connector_gap_rows_fn(cfg)
        checks.append(
            make_check(
                'connector-readiness',
                'error' if gaps else 'ok',
                (
                    'One or more configured connectors are missing required '
                    'runtime fields or use unsupported connector types.'
                    if gaps
                    else 'Configured connectors include the required runtime fields.'
                ),
                **({'gaps': gaps} if gaps else {}),
            ),
        )

        missing_requirements = cls.missing_requirement_rows(
            cfg=cfg,
            package_available=package_available,
        )
        checks.append(
            make_check(
                'optional-dependencies',
                'error' if missing_requirements else 'ok',
                (
                    'Configured connectors require optional dependencies that '
                    'are not installed.'
                    if missing_requirements
                    else (
                        'No missing optional dependencies were detected for '
                        'configured connectors.'
                    )
                ),
                **(
                    {'missing_requirements': missing_requirements}
                    if missing_requirements
                    else {}
                ),
            ),
        )
        return checks

    # -- Static Methods -- #

    @staticmethod
    def netcdf_available(
        *,
        package_available: Callable[[str], bool],
    ) -> bool:
        """
        Return whether netCDF support dependencies are installed.

        Parameters
        ----------
        package_available : Callable[[str], bool]
            A function that returns whether a package is available.

        Returns
        -------
        bool
            ``True`` if netCDF support dependencies are installed, ``False``
            if not.
        """
        return package_available('xarray') and (
            package_available('netCDF4') or package_available('h5netcdf')
        )

    @staticmethod
    def requirement_row(
        *,
        connector: str,
        detected_format: str | None = None,
        detected_queue_service: str | None = None,
        detected_scheme: str | None = None,
        reason: str,
        requirement: RequirementSpec,
        role: str,
    ) -> ReadinessRow:
        """Return one missing-requirement row."""
        row: ReadinessRow = {
            'connector': connector,
            'extra': requirement.extra or '',
            'guidance': ReadinessSupportPolicy.missing_requirement_guidance(
                detected_format=detected_format,
                detected_queue_service=detected_queue_service,
                detected_scheme=detected_scheme,
                package=requirement.package,
                extra=requirement.extra,
            ),
            'missing_package': requirement.package,
            'reason': reason,
            'role': role,
        }
        if detected_format is not None:
            row['detected_format'] = detected_format
        if detected_queue_service is not None:
            row['detected_queue_service'] = detected_queue_service
        if detected_scheme is not None:
            row['detected_scheme'] = detected_scheme
        return row
