"""
:mod:`etlplus.runtime.readiness._base` module.

Shared readiness utility methods for :class:`ReadinessReportBuilder`.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from collections.abc import Mapping
from importlib.util import find_spec
from pathlib import Path
from typing import Any
from typing import cast
from urllib.parse import urlsplit

from ..._config import Config
from ...connector import Connector
from ...file import File
from ...file import FileFormat
from ...storage import StorageScheme
from ...utils import deep_substitute
from ...utils import maybe_mapping
from ...utils._types import StrAnyMap
from ._support import SUPPORTED_PYTHON_RANGE
from ._support import TOKEN_PATTERN
from ._support import CheckStatus
from ._support import ReadinessRow
from ._support import ResolvedConfigContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessBaseMixin',
    # Functions
    'coerce_storage_scheme',
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


def _dedupe_rows(
    rows: list[ReadinessRow],
) -> list[ReadinessRow]:
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


def _iter_connectors(
    cfg: Config,
) -> Iterator[tuple[str, Connector]]:
    """Yield source and target connectors tagged with their role."""
    yield from (('source', connector) for connector in cfg.sources)
    yield from (('target', connector) for connector in cfg.targets)


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


# SECTION: FUNCTIONS ======================================================== #


def coerce_connector_storage_scheme(
    value: str,
) -> str | None:
    """
    Return one normalized storage scheme from raw connector-type text.

    This is a best-effort coercion that accepts some common connector type
    strings and attempts to map them to a recognized storage scheme.
    Recognized storage schemes are normalized to their canonical lowercase
    form.

    Parameters
    ----------
    value : str
        Raw connector type string to coerce as a storage scheme.

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
        The path to coerce as a storage scheme.

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


# SECTION: CLASSES ========================================================== #


class ReadinessBaseMixin:
    """Shared readiness utility mixin for the public builder facade."""

    # -- Static Methods -- #

    @staticmethod
    def coerce_connector_storage_scheme(
        value: str,
    ) -> str | None:
        """
        Return one normalized storage scheme from raw connector-type text.

        This is a best-effort coercion that accepts some common connector type
        strings and attempts to map them to a recognized storage scheme.
        Recognized storage schemes are normalized to their canonical lowercase
        form.

        Parameters
        ----------
        value : str
            Raw connector type string to coerce as a storage scheme.

        Returns
        -------
        str | None
            The normalized storage scheme name if coercion is successful, or
            ``None`` if the value cannot be coerced as a known storage scheme.
        """
        return coerce_connector_storage_scheme(value)

    @staticmethod
    def coerce_storage_scheme(
        path: str,
    ) -> str | None:
        """
        Return one normalized storage scheme for *path* when present.

        Parameters
        ----------
        path : str
            The path to coerce as a storage scheme.

        Returns
        -------
        str | None
            The normalized storage scheme name if present, or ``None`` if not.
        """
        return coerce_storage_scheme(path)

    @staticmethod
    def collect_substitution_tokens(
        value: Any,
    ) -> set[str]:
        """
        Return unresolved ``${VAR}`` token names found in nested values.

        Parameters
        ----------
        value : Any
            The value to search for substitution tokens. Can be any nested
            combination of mappings, sequences, and scalars.

        Returns
        -------
        set[str]
            The set of unique token names found in the value. Token names are
            returned without the ``${}`` delimiters. For example, if the value
            contains the string ``"Database URL: ${DB_URL}"``, the returned set
            will include the token name ``"DB_URL"``.
        """
        tokens: set[str] = set()

        def _walk(node: Any) -> None:
            match node:
                case str():
                    for match in TOKEN_PATTERN.findall(node):
                        tokens.add(match)
                case Mapping():
                    for inner in node.values():
                        _walk(inner)
                case list() | tuple() | set() | frozenset() as seq:
                    for inner in seq:
                        _walk(inner)
                case _:
                    return

        _walk(value)
        return tokens

    @staticmethod
    def connector_gap_guidance(
        *,
        api_reference: str | None = None,
        issue: str,
    ) -> str | None:
        """
        Return one actionable guidance string for a blocking connector gap.

        Parameters
        ----------
        api_reference : str | None
            The API reference associated with the connector gap, if any.
        issue : str
            The specific issue causing the connector gap.

        Returns
        -------
        str | None
            An actionable guidance string if the issue is recognized, or ``None``
            if not.
        """
        return _connector_gap_guidance(
            api_reference=api_reference,
            issue=issue,
        )

    @staticmethod
    def dedupe_rows(
        rows: list[ReadinessRow],
    ) -> list[ReadinessRow]:
        """
        Return rows with duplicates removed while preserving order.

        Parameters
        ----------
        rows : list[ReadinessRow]
            The list of rows to deduplicate.

        Returns
        -------
        list[ReadinessRow]
            The deduplicated list of rows.
        """
        return _dedupe_rows(rows)

    @staticmethod
    def effective_environment(
        cfg: Config,
        env: Mapping[str, str] | None,
    ) -> dict[str, str]:
        """
        Return the merged environment used for config substitution.

        Parameters
        ----------
        cfg : Config
            The pipeline configuration whose profile may define base
            environment variables for substitution.
        env : Mapping[str, str] | None
            An optional external environment mapping to merge with the profile
            environment. If ``None``, the current process environment will be
            used.

        Returns
        -------
        dict[str, str]
            The merged environment mapping, where variables from *env* take
            precedence over those from the config profile. If both the profile
            and *env* define the same variable, the value from *env* will be
            used in substitutions.
        """
        base_env = dict(getattr(cfg.profile, 'env', {}) or {})
        external_env = dict(env) if env is not None else dict(os.environ)
        return base_env | external_env

    @staticmethod
    def iter_connectors(
        cfg: Config,
    ) -> Iterator[tuple[str, Connector]]:
        """
        Yield source and target connectors tagged with their role.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.

        Returns
        -------
        Iterator[tuple[str, Connector]]
            An iterator over tuples of role and connector.

        Yields
        ------
        tuple[str, Connector]
            Tuples of role ("source" or "target") and connector objects from the
            configuration.
        """
        yield from _iter_connectors(cfg)

    @staticmethod
    def load_raw_config(
        config_path: str,
    ) -> StrAnyMap:
        """
        Load raw YAML config and require a mapping root.

        Parameters
        ----------
        config_path : str
            The path to the YAML configuration file.

        Returns
        -------
        StrAnyMap
            The loaded configuration as a dictionary.

        Raises
        ------
        TypeError
            If the YAML root is not a mapping/object.
        """
        raw = File(Path(config_path), FileFormat.YAML).read()
        mapping = maybe_mapping(raw)
        if mapping is None:
            raise TypeError('Pipeline YAML must have a mapping/object root')
        return dict(mapping)

    @staticmethod
    def make_check(
        name: str,
        status: CheckStatus,
        message: str,
        **details: Any,
    ) -> dict[str, Any]:
        """Return one readiness check row."""
        payload: dict[str, Any] = {
            'name': name,
            'status': status,
            'message': message,
        }
        payload.update(details)
        return payload

    @staticmethod
    def missing_requirement_guidance(
        *,
        detected_format: str | None = None,
        detected_scheme: str | None = None,
        package: str,
        extra: str | None,
    ) -> str:
        """
        Return one actionable remediation string for a missing dependency.

        Parameters
        ----------
        detected_format : str | None
            The file format detected in the connector configuration, if any.
        detected_scheme : str | None
            The storage scheme detected in the connector configuration, if any.
        package : str
            The missing package that should be installed.
        extra : str | None
            The ETLPlus extra that includes the missing package, if any.

        Returns
        -------
        str
            An actionable remediation string for the missing dependency.
        """
        return _missing_requirement_guidance(
            detected_format=detected_format,
            detected_scheme=detected_scheme,
            package=package,
            extra=extra,
        )

    @staticmethod
    def package_available(
        module_name: str,
    ) -> bool:
        """
        Return whether *module_name* is importable without importing it.

        This is a best-effort check that returns ``True`` if the module can be
        imported, and ``False`` if it cannot. It does not attempt to import the
        module, but instead uses importlib metadata to check for its presence.

        Parameters
        ----------
        module_name : str
            The name of the module to check for availability.

        Returns
        -------
        bool
            ``True`` if the module is available, ``False`` if not.
        """
        try:
            return find_spec(module_name) is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            return False

    # -- Class Methods -- #

    @classmethod
    def overall_status(
        cls,
        checks: list[dict[str, Any]],
    ) -> CheckStatus:
        """
        Return aggregate status from individual check rows.

        Parameters
        ----------
        checks : list[dict[str, Any]]
            A list of individual check rows.

        Returns
        -------
        CheckStatus
            The overall status derived from the individual checks.
        """
        statuses = {cast(CheckStatus, check['status']) for check in checks}
        if 'error' in statuses:
            return 'error'
        if 'warn' in statuses:
            return 'warn'
        return 'ok'

    @classmethod
    def python_version(cls) -> str:
        """
        Return the current interpreter version as dotted text.

        Returns
        -------
        str
            The current Python interpreter version.
        """
        return (
            f'{sys.version_info.major}.'
            f'{sys.version_info.minor}.'
            f'{sys.version_info.micro}'
        )

    @classmethod
    def resolve_config_context(
        cls,
        raw: StrAnyMap,
        *,
        env: Mapping[str, str] | None,
    ) -> ResolvedConfigContext:
        """
        Return resolved config state shared by config readiness checks.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.

        Returns
        -------
        ResolvedConfigContext
            The resolved configuration context.
        """
        cfg = Config.from_dict(raw)
        effective_env = cls.effective_environment(cfg, env)
        resolved = cast(StrAnyMap, deep_substitute(raw, cfg.vars, effective_env))
        unresolved_tokens = sorted(cls.collect_substitution_tokens(resolved))
        return ResolvedConfigContext(
            raw=raw,
            effective_env=effective_env,
            unresolved_tokens=unresolved_tokens,
            resolved_raw=resolved,
            resolved_cfg=(None if unresolved_tokens else Config.from_dict(resolved)),
        )

    @classmethod
    def supported_python_check(
        cls,
    ) -> dict[str, Any]:
        """
        Return the runtime Python compatibility check.

        Returns
        -------
        dict[str, Any]
            The Python compatibility check result.
        """
        version = cls.python_version()
        minimum, maximum = SUPPORTED_PYTHON_RANGE
        supported = minimum <= sys.version_info[:2] < maximum
        if supported:
            return cls.make_check(
                'python-version',
                'ok',
                f'Python {version} is within the supported ETLPlus runtime range.',
                version=version,
            )
        return cls.make_check(
            'python-version',
            'error',
            (
                f'Python {version} is outside the supported ETLPlus runtime range '
                f'(>={minimum[0]}.{minimum[1]},<{maximum[0]}.{maximum[1]}).'
            ),
            version=version,
        )

    @classmethod
    def token_reference_rows(
        cls,
        value: Any,
    ) -> list[dict[str, Any]]:
        """
        Return unresolved token rows with stable dotted and indexed paths.

        Parameters
        ----------
        value : Any
            The value to inspect for unresolved tokens.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries containing token names and their paths.
        """
        paths_by_name: dict[str, set[str]] = {}

        def _walk(node: Any, path: str = '') -> None:
            match node:
                case str():
                    for match in TOKEN_PATTERN.findall(node):
                        paths_by_name.setdefault(match, set()).add(path or '<root>')
                case Mapping():
                    for key, inner in node.items():
                        key_text = str(key)
                        next_path = f'{path}.{key_text}' if path else key_text
                        _walk(inner, next_path)
                case list() | tuple() as seq:
                    for index, inner in enumerate(seq):
                        next_path = f'{path}[{index}]' if path else f'[{index}]'
                        _walk(inner, next_path)
                case set() | frozenset():
                    for index, inner in enumerate(sorted(node, key=repr)):
                        next_path = f'{path}[{index}]' if path else f'[{index}]'
                        _walk(inner, next_path)
                case _:
                    return

        _walk(value)
        return [
            {'name': name, 'paths': sorted(paths)}
            for name, paths in sorted(paths_by_name.items())
        ]
