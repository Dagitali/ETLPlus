"""
:mod:`etlplus.runtime._readiness_connectors` module.

Connector and optional-dependency readiness helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

from .._config import Config
from ..connector import Connector
from ..connector import DataConnectorType
from ..storage import StorageScheme
from ._readiness_support import _FORMAT_EXTRA_REQUIREMENTS
from ._readiness_support import _SCHEME_EXTRA_REQUIREMENTS
from ._readiness_support import ReadinessRow
from ._readiness_support import _RequirementSpec

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'connector_gap_rows',
    'connector_readiness_checks',
    'connector_type_choices',
    'connector_type_guidance',
    'missing_requirement_rows',
    'netcdf_available',
    'requirement_available',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _connector_gap_guidance(
    *,
    api_reference: str | None = None,
    issue: str,
) -> str | None:
    """Return one actionable guidance string for a blocking connector gap."""
    match issue:
        case 'missing path':
            return 'Set "path" to a local path or storage URI for this file connector.'
        case 'missing url or api reference':
            return (
                'Set "url" to a reachable endpoint or "api" to a configured '
                'top-level API name.'
            )
        case 'missing connection_string':
            return 'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
        case issue_text if issue_text.startswith('unknown api reference: '):
            if api_reference:
                return (
                    f'Define "{api_reference}" under top-level "apis" or update '
                    'the connector "api" reference.'
                )
            return 'Define the referenced API under top-level "apis".'
        case _:
            return None


def _dedupe_rows(rows: list[ReadinessRow]) -> list[ReadinessRow]:
    """Return rows with duplicates removed while preserving order."""
    unique_rows: list[ReadinessRow] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        key = (
            str(row['connector']),
            str(row['role']),
            str(row['missing_package']),
            str(row['reason']),
            str(row['extra']),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def _missing_requirement_guidance(
    *,
    detected_format: str | None = None,
    detected_scheme: str | None = None,
    package: str,
    extra: str | None,
) -> str:
    """Return one actionable remediation string for a missing dependency."""
    install_hint = (
        f'Install {package} directly or install the ETLPlus "{extra}" extra.'
        if extra
        else f'Install {package}.'
    )
    if detected_format == 'nc':
        return (
            'Install xarray plus one of netCDF4 or h5netcdf, or install the '
            'ETLPlus "file" extra.'
        )
    if detected_format is not None:
        return f'{install_hint} Required for "{detected_format}" file format.'
    if detected_scheme is not None:
        return f'{install_hint} Required for "{detected_scheme}" storage paths.'
    return install_hint


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
        row['guidance'] = _connector_gap_guidance(
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


def _iter_connectors(
    cfg: Config,
) -> Iterator[tuple[str, Connector]]:
    """Yield source and target connectors tagged with their role."""
    yield from (('source', connector) for connector in cfg.sources)
    yield from (('target', connector) for connector in cfg.targets)


def _requirement_row(
    *,
    connector: str,
    detected_format: str | None = None,
    detected_scheme: str | None = None,
    reason: str,
    requirement: _RequirementSpec,
    role: str,
) -> ReadinessRow:
    """Return one missing-requirement row."""
    row: ReadinessRow = {
        'connector': connector,
        'extra': requirement.extra or '',
        'guidance': _missing_requirement_guidance(
            detected_format=detected_format,
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
    if detected_scheme is not None:
        row['detected_scheme'] = detected_scheme
    return row


# SECTION: FUNCTIONS ======================================================== #


def coerce_connector_storage_scheme(
    value: str,
) -> str | None:
    """
    Return one normalized storage scheme from raw connector-type text.

    Parameters
    ----------
    value : str
        The raw connector type string to coerce as a storage scheme.

    Returns
    -------
    str | None
        The normalized storage scheme name if coercion is successful, or
        ``None`` if the value cannot be coerced as a known storage scheme.
    """
    if not value:
        return None
    try:
        return str(StorageScheme.coerce(value))
    except ValueError:
        return None


def coerce_storage_scheme(
    path: str,
) -> str | None:
    """
    Return one normalized storage scheme for *path* when present.

    Parameters
    ----------
    path : str
        The path to check for a storage scheme.

    Returns
    -------
    str | None
        The normalized storage scheme name if present, or ``None`` if not.
    """
    if '://' not in path:
        return None
    parsed = urlsplit(path)
    if not parsed.scheme:
        return None
    try:
        return str(StorageScheme.coerce(parsed.scheme))
    except ValueError:
        return parsed.scheme.lower()


def connector_gap_rows(
    cfg: Config,
) -> list[ReadinessRow]:
    """
    Return connector configuration gaps that will block execution.

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
    for role, connector in _iter_connectors(cfg):
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


def connector_readiness_checks(
    cfg: Config,
    *,
    connector_gap_rows_fn: Callable[[Config], list[ReadinessRow]],
    make_check: Callable[..., dict[str, Any]],
    missing_requirement_rows_fn: Callable[[Config], list[ReadinessRow]],
) -> list[ReadinessRow]:
    """
    Return connector configuration and dependency readiness checks.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    connector_gap_rows_fn : Callable[[Config], list[ReadinessRow]]
        A function that returns a list of connector configuration gaps.
    make_check : Callable[..., dict[str, Any]]
        A function that creates a readiness check dictionary.
    missing_requirement_rows_fn : Callable[[Config], list[ReadinessRow]]
        A function that returns a list of missing optional dependencies.

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

    missing_requirements = missing_requirement_rows_fn(cfg)
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
    normalized = connector_type_str.strip().lower()
    if not normalized:
        return f'Set type to one of: {supported}.'
    if coerce_connector_storage_scheme(normalized) is not None:
        return (
            f'"{normalized}" is a storage scheme, not a connector type. '
            'Use connector type "file" and keep the provider in the path '
            'or URI scheme.'
        )
    return f'Use one of the supported connector types: {supported}.'


def missing_requirement_rows(
    *,
    cfg: Config,
    netcdf_available_fn: Callable[[], bool],
    requirement_available_fn: Callable[[_RequirementSpec], bool],
) -> list[ReadinessRow]:
    """
    Return missing optional dependency rows for configured connectors.

    Parameters
    ----------
    cfg : Config
        The configuration object containing the connectors.
    netcdf_available_fn : Callable[[], bool]
        A function that returns whether netCDF support dependencies are installed.
    requirement_available_fn : Callable[[_RequirementSpec], bool]
        A function that returns whether the dependencies for a given
        requirement are installed.

    Returns
    -------
    list[ReadinessRow]
        A list of dictionaries representing the missing optional dependencies
        for the configured connectors.
    """
    rows: list[ReadinessRow] = []
    for role, connector in _iter_connectors(cfg):
        connector_name = str(getattr(connector, 'name', '<unnamed>'))
        path = getattr(connector, 'path', None)
        format_name = str(getattr(connector, 'format', '') or '').lower()

        if path:
            scheme = coerce_storage_scheme(path)
            requirement = _SCHEME_EXTRA_REQUIREMENTS.get(scheme or '')
            if scheme and requirement and not requirement_available_fn(requirement):
                rows.append(
                    _requirement_row(
                        connector=connector_name,
                        detected_scheme=scheme,
                        reason=f'{scheme} storage path requires {requirement.package}',
                        requirement=requirement,
                        role=role,
                    ),
                )

        if format_name == 'nc':
            if not netcdf_available_fn():
                rows.append(
                    _requirement_row(
                        connector=connector_name,
                        detected_format='nc',
                        reason='nc format requires xarray and netCDF4 or h5netcdf',
                        requirement=_RequirementSpec(
                            modules=('xarray', 'netCDF4', 'h5netcdf'),
                            package='xarray/netCDF4',
                            extra='file',
                        ),
                        role=role,
                    ),
                )
            continue

        requirement = _FORMAT_EXTRA_REQUIREMENTS.get(format_name)
        if requirement and not requirement_available_fn(requirement):
            rows.append(
                _requirement_row(
                    connector=connector_name,
                    detected_format=format_name,
                    reason=f'{format_name} format requires {requirement.package}',
                    requirement=requirement,
                    role=role,
                ),
            )

    return _dedupe_rows(rows)


def netcdf_available(
    *,
    package_available: Callable[[str], bool],
) -> bool:
    """
    Return whether netCDF support dependencies are installed.

    Parameters
    ----------
    package_available : Callable[[str], bool]
        A function that checks if a package is available.

    Returns
    -------
    bool
        True if netCDF support dependencies are installed, False otherwise.
    """
    return package_available('xarray') and (
        package_available('netCDF4') or package_available('h5netcdf')
    )


def requirement_available(
    requirement: _RequirementSpec,
    *,
    package_available: Callable[[str], bool],
) -> bool:
    """
    Return whether any module for one requirement is importable.

    Parameters
    ----------
    requirement : _RequirementSpec
        The requirement specification containing the modules to check.
    package_available : Callable[[str], bool]
        A function that checks if a package is available.

    Returns
    -------
    bool
        True if any module for the requirement is importable, False otherwise.
    """
    return any(package_available(module_name) for module_name in requirement.modules)
