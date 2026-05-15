"""
:mod:`etlplus.runtime.readiness._connectors` module.

Connector and optional-dependency readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..._config import Config
from ...connector import DataConnectorType
from ...queue import QueueService
from ...utils import TextNormalizer
from ._base import ReadinessSupportPolicy
from ._support import DATABASE_PROVIDER_EXTRA_REQUIREMENTS
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


# SECTION: INTERNAL DATA CLASSES ============================================ #


@dataclass(frozen=True, slots=True)
class _ResolvedConnector:
    """Normalized connector state reused by readiness policies."""

    connector: object
    database_dataset: str | None
    database_project: str | None
    database_provider: str
    format_name: str
    name: str
    path: str | None
    queue_service: str
    role: str
    type_name: str


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


def _resolve_queue_service(
    connector: object,
) -> str:
    """Return one normalized queue service name for *connector*."""
    queue_service_raw = TextNormalizer.normalize(
        str(getattr(connector, 'service', '') or ''),
    )
    coerced_service = QueueService.try_coerce(queue_service_raw)
    return coerced_service.value if coerced_service else queue_service_raw


def _iter_connectors(
    cfg: Config,
) -> tuple[_ResolvedConnector, ...]:
    """Return normalized connector rows reused across readiness policies."""
    return tuple(
        _ResolvedConnector(
            connector=connector,
            database_dataset=dataset
            if isinstance(dataset := getattr(connector, 'dataset', None), str)
            else None,
            database_project=project
            if isinstance(project := getattr(connector, 'project', None), str)
            else None,
            database_provider=TextNormalizer.normalize(
                str(getattr(connector, 'provider', '') or ''),
            ),
            format_name=TextNormalizer.normalize(
                str(getattr(connector, 'format', '') or ''),
            ),
            name=str(getattr(connector, 'name', '<unnamed>')),
            path=path
            if isinstance(path := getattr(connector, 'path', None), str)
            else None,
            queue_service=_resolve_queue_service(connector),
            role=role,
            type_name=str(getattr(connector, 'type', '') or ''),
        )
        for role, connector in ReadinessSupportPolicy.iter_connectors(cfg)
    )


# SECTION: FUNCTIONS ======================================================== #


def connector_type_choices() -> tuple[str, ...]:
    """
    Return the supported connector type names.

    Returns
    -------
    tuple[str, ...]
        A tuple of supported connector type names.
    """
    return DataConnectorType.choices()


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
        for resolved in _iter_connectors(cfg):
            coerced_type = DataConnectorType.try_coerce(resolved.type_name)

            if coerced_type is None:
                gaps.append(
                    _connector_gap_row(
                        connector=resolved.name,
                        connector_type_str=resolved.type_name,
                        issue='unsupported type',
                        role=resolved.role,
                        supported_types=supported_types,
                    ),
                )
                continue

            match coerced_type:
                case DataConnectorType.FILE if not resolved.path:
                    gaps.append(
                        _connector_gap_row(
                            connector=resolved.name,
                            connector_type_str=resolved.type_name,
                            issue='missing path',
                            role=resolved.role,
                        ),
                    )
                case DataConnectorType.API:
                    api_reference_value = getattr(resolved.connector, 'api', None)
                    api_reference = (
                        api_reference_value
                        if isinstance(api_reference_value, str)
                        else None
                    )
                    if (
                        not getattr(resolved.connector, 'url', None)
                        and not api_reference
                    ):
                        gaps.append(
                            _connector_gap_row(
                                connector=resolved.name,
                                connector_type_str=resolved.type_name,
                                issue='missing url or api reference',
                                role=resolved.role,
                            ),
                        )
                    elif api_reference and api_reference not in cfg.apis:
                        gaps.append(
                            _connector_gap_row(
                                api_reference=api_reference,
                                connector=resolved.name,
                                connector_type_str=resolved.type_name,
                                issue=f'unknown api reference: {api_reference}',
                                role=resolved.role,
                            ),
                        )
                case DataConnectorType.DATABASE if not getattr(
                    resolved.connector,
                    'connection_string',
                    None,
                ):
                    gaps.append(
                        _connector_gap_row(
                            connector=resolved.name,
                            connector_type_str=resolved.type_name,
                            issue=(
                                'missing connection_string or bigquery '
                                'project/dataset'
                                if resolved.database_provider == 'bigquery'
                                and not (
                                    resolved.database_project
                                    and resolved.database_dataset
                                )
                                else 'missing connection_string'
                            ),
                            role=resolved.role,
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
        for resolved in _iter_connectors(cfg):
            if resolved.database_provider in DATABASE_PROVIDER_EXTRA_REQUIREMENTS:
                requirement = DATABASE_PROVIDER_EXTRA_REQUIREMENTS[
                    resolved.database_provider
                ]
                if not requirement.is_available(
                    availability_checker=package_available,
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=resolved.name,
                            detected_database_provider=resolved.database_provider,
                            reason=(
                                f'{resolved.database_provider} database connector '
                                f'requires {requirement.package}'
                            ),
                            requirement=requirement,
                            role=resolved.role,
                        ),
                    )

            if resolved.path:
                scheme = ReadinessSupportPolicy.coerce_storage_scheme(resolved.path)
                scheme_requirement = SCHEME_EXTRA_REQUIREMENTS.get(scheme or '')
                if (
                    scheme
                    and scheme_requirement
                    and not scheme_requirement.is_available(
                        availability_checker=package_available,
                    )
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=resolved.name,
                            detected_scheme=scheme,
                            reason=(
                                f'{scheme} storage path requires '
                                f'{scheme_requirement.package}'
                            ),
                            requirement=scheme_requirement,
                            role=resolved.role,
                        ),
                    )

            if (
                DataConnectorType.try_coerce(resolved.type_name)
                is DataConnectorType.QUEUE
                and resolved.queue_service in QUEUE_SERVICE_EXTRA_REQUIREMENTS
            ):
                requirement = QUEUE_SERVICE_EXTRA_REQUIREMENTS[resolved.queue_service]
                if not requirement.is_available(
                    availability_checker=package_available,
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=resolved.name,
                            detected_queue_service=resolved.queue_service,
                            reason=(
                                f'{resolved.queue_service} queue connector requires '
                                f'{requirement.package}'
                            ),
                            requirement=requirement,
                            role=resolved.role,
                        ),
                    )

            if resolved.format_name == 'nc':
                if not cls.netcdf_available(
                    package_available=package_available,
                ):
                    rows.append(
                        cls.requirement_row(
                            connector=resolved.name,
                            detected_format='nc',
                            reason=(
                                'nc format requires xarray and netCDF4 or h5netcdf'
                            ),
                            requirement=RequirementSpec(
                                modules=('xarray', 'netCDF4', 'h5netcdf'),
                                package='xarray/netCDF4',
                                extra='file',
                            ),
                            role=resolved.role,
                        ),
                    )
                continue

            format_requirement = FORMAT_EXTRA_REQUIREMENTS.get(resolved.format_name)
            if format_requirement and not format_requirement.is_available(
                availability_checker=package_available,
            ):
                rows.append(
                    cls.requirement_row(
                        connector=resolved.name,
                        detected_format=resolved.format_name,
                        reason=(
                            f'{resolved.format_name} format requires '
                            f'{format_requirement.package}'
                        ),
                        requirement=format_requirement,
                        role=resolved.role,
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
        detected_database_provider: str | None = None,
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
                detected_database_provider=detected_database_provider,
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
        if detected_database_provider is not None:
            row['detected_database_provider'] = detected_database_provider
        if detected_format is not None:
            row['detected_format'] = detected_format
        if detected_queue_service is not None:
            row['detected_queue_service'] = detected_queue_service
        if detected_scheme is not None:
            row['detected_scheme'] = detected_scheme
        return row
