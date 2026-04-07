"""
:mod:`etlplus.runtime._readiness` module.

Runtime readiness checks for the CLI and future execution surfaces.
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

from .. import __version__
from .._config import Config
from ..connector import Connector
from ..connector import DataConnectorType as _DataConnectorType
from ..file import File
from ..file import FileFormat
from ..storage import StorageScheme
from ..utils import deep_substitute
from ..utils import maybe_mapping
from ..utils._types import StrAnyMap
from . import _readiness_connectors as _connectors
from . import _readiness_providers as _providers
from . import _readiness_strict as _strict
from ._readiness_support import _SUPPORTED_PYTHON_RANGE
from ._readiness_support import _TOKEN_PATTERN
from ._readiness_support import CheckStatus
from ._readiness_support import _RequirementSpec
from ._readiness_support import _ResolvedConfigContext

DataConnectorType = _DataConnectorType

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]


# SECTION: CLASSES ========================================================== #


class ReadinessReportBuilder:
    """Shared builder for ETLPlus runtime readiness reports."""

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
        if not value:
            return None
        try:
            return str(StorageScheme.coerce(value))
        except ValueError:
            return None

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
        if '://' not in path:
            return None
        parsed = urlsplit(path)
        if not parsed.scheme:
            return None
        try:
            return str(StorageScheme.coerce(parsed.scheme))
        except ValueError:
            return parsed.scheme.lower()

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
                    for match in _TOKEN_PATTERN.findall(node):
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
        match issue:
            case 'missing path':
                return (
                    'Set "path" to a local path or storage URI for this file connector.'
                )
            case 'missing url or api reference':
                return (
                    'Set "url" to a reachable endpoint or "api" to a configured '
                    'top-level API name.'
                )
            case 'missing connection_string':
                return (
                    'Set "connection_string" to a database DSN or SQLAlchemy-style URL.'
                )
            case issue_text if issue_text.startswith('unknown api reference: '):
                if api_reference:
                    return (
                        f'Define "{api_reference}" under top-level "apis" or update '
                        'the connector "api" reference.'
                    )
                return 'Define the referenced API under top-level "apis".'
            case _:
                return None

    @staticmethod
    def dedupe_rows(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Return rows with duplicates removed while preserving order.

        Parameters
        ----------
        rows : list[dict[str, Any]]
            The list of rows to deduplicate.

        Returns
        -------
        list[dict[str, Any]]
            The deduplicated list of rows.
        """
        unique_rows: list[dict[str, Any]] = []
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
        yield from (('source', connector) for connector in cfg.sources)
        yield from (('target', connector) for connector in cfg.targets)

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
    def aws_env_hint_present(
        cls,
        env: Mapping[str, str],
    ) -> bool:
        """
        Return whether common AWS credential-chain env hints are present.

        The presence of any of these environment variables is a strong signal that
        the user intends to use AWS credentials, even if the variables are not
        sufficient for a fully explicit configuration. This is used to provide more
        targeted guidance when an S3 path is detected but no AWS credential hints
        are found in the environment. The check is intentionally broad to avoid
        false negatives for users relying on shared config files, container
        credentials, or instance metadata, which may not have a single specific
        environment variable set.

        Parameters
        ----------
        env : Mapping[str, str]
            The environment mapping to check for AWS credential hints, typically
            ``os.environ`` or a similar mapping.

        Returns
        -------
        bool
            ``True`` if any common AWS credential-chain environment variable is
            present, ``False`` if not.
        """
        return _providers.aws_env_hint_present(env)

    @classmethod
    def azure_authority_has_account_host(
        cls,
        path: str,
    ) -> bool:
        """
        Return whether one Azure storage path authority embeds an account host.

        For Azure storage paths, the authority component of the URI may include an
        account host, which can serve as a bootstrap credential hint. For example,
        in the path ``https://myaccount.blob.core.windows.net/mycontainer/myblob``,
        the authority component is ``myaccount.blob.core.windows.net``.

        Parameters
        ----------
        path : str
            The path to check for an Azure storage authority with an account host.

        Returns
        -------
        bool
            ``True`` if the authority component of the Azure storage path includes
            an account host, ``False`` if not.

        """
        return _providers.azure_authority_has_account_host(path)

    @classmethod
    def connector_gap_rows(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return connector configuration gaps that will block execution.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the configuration gaps.
        """
        return _connectors.connector_gap_rows(cfg)

    @classmethod
    def connector_readiness_checks(
        cls,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return connector configuration and dependency readiness checks.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the readiness checks.
        """
        return _connectors.connector_readiness_checks(
            cfg,
            connector_gap_rows_fn=cls.connector_gap_rows,
            make_check=cls.make_check,
            missing_requirement_rows_fn=lambda inner_cfg: cls.missing_requirement_rows(
                cfg=inner_cfg,
            ),
        )

    @classmethod
    def connector_type(
        cls,
        connector_type_str: str,
    ) -> _DataConnectorType | None:
        """
        Return one coerced connector type or ``None`` when unsupported.

        Parameters
        ----------
        connector_type_str : str
            The connector type string to coerce.

        Returns
        -------
        _DataConnectorType | None
            The coerced connector type or ``None`` if unsupported.
        """
        return _connectors.connector_type(connector_type_str)

    @classmethod
    def connector_type_choices(
        cls,
    ) -> tuple[str, ...]:
        """
        Return the supported connector type names.

        Returns
        -------
        tuple[str, ...]
            A tuple of supported connector type names.
        """
        return _connectors.connector_type_choices()

    @classmethod
    def connector_type_guidance(
        cls,
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
        return _connectors.connector_type_guidance(connector_type_str)

    @classmethod
    def explicit_aws_credential_gap(
        cls,
        env: Mapping[str, str],
    ) -> dict[str, Any] | None:
        """
        Return one AWS env error row for incomplete explicit credentials.

        This check looks for the presence of partial explicit AWS credential
        environment variables that indicate an attempt at explicit credential
        configuration, but the variables are not sufficient for a complete explicit
        configuration. The check is intentionally specific to avoid false positives
        for users who are not attempting explicit credential configuration, as the
        presence of any of these variables is a strong signal of that intent. If
        both ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` are not set, but
        ``AWS_SESSION_TOKEN`` is set, it indicates an incomplete configuration.

        Parameters
        ----------
        env : Mapping[str, str]
            The environment variables to check.

        Returns
        -------
        dict[str, Any] | None
            An error row for incomplete explicit AWS credentials, or ``None`` if no
            issues are found.
        """
        return _providers.explicit_aws_credential_gap(env)

    @classmethod
    def missing_requirement_rows(
        cls,
        *,
        cfg: Config,
    ) -> list[dict[str, Any]]:
        """
        Return missing optional dependency rows for configured connectors.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the missing optional dependencies
            for the configured connectors.
        """
        return _connectors.missing_requirement_rows(
            cfg=cfg,
            netcdf_available_fn=cls.netcdf_available,
            requirement_available_fn=cls.requirement_available,
        )

    @classmethod
    def netcdf_available(cls) -> bool:
        """
        Return whether netCDF support dependencies are installed.

        Returns
        -------
        bool
            True if netCDF support dependencies are installed, False otherwise.
        """
        return _connectors.netcdf_available(package_available=cls.package_available)

    @classmethod
    def provider_environment_checks(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """
        Return provider-specific environment readiness checks.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors and profile.
        env : Mapping[str, str]
            The environment variables to consider for provider readiness.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the provider-specific
            environment readiness checks.

        """
        return _providers.provider_environment_checks(
            cfg=cfg,
            env=env,
            make_check=cls.make_check,
            provider_environment_rows_fn=lambda inner_cfg, inner_env: (
                cls.provider_environment_rows(cfg=inner_cfg, env=inner_env)
            ),
        )

    @classmethod
    def provider_environment_rows(
        cls,
        *,
        cfg: Config,
        env: Mapping[str, str],
    ) -> list[dict[str, Any]]:
        """
        Return provider-specific environment gaps for configured connectors.

        Parameters
        ----------
        cfg : Config
            The configuration object containing the connectors.
        env : Mapping[str, str]
            The environment variables to check for provider-specific gaps.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the provider-specific environment
            gaps.
        """
        return _providers.provider_environment_rows(cfg=cfg, env=env)

    @classmethod
    def requirement_available(
        cls,
        requirement: _RequirementSpec,
    ) -> bool:
        """
        Return whether any module for one requirement is importable.

        Parameters
        ----------
        requirement : _RequirementSpec
            The requirement specification to check.

        Returns
        -------
        bool
            ``True`` if any module for the requirement is importable,
            ``False`` if not.
        """
        return _connectors.requirement_available(
            requirement,
            package_available=cls.package_available,
        )

    @classmethod
    def requirement_row(
        cls,
        *,
        connector: str,
        detected_format: str | None = None,
        detected_scheme: str | None = None,
        reason: str,
        requirement: _RequirementSpec,
        role: str,
    ) -> dict[str, Any]:
        """
        Return one missing-requirement row.

        Parameters
        ----------
        connector : str
            The name of the connector.
        detected_format : str | None, optional
            The detected format, if any.
        detected_scheme : str | None, optional
            The detected scheme, if any.
        reason : str
            The reason for the missing requirement.
        requirement : _RequirementSpec
            The requirement specification.
        role : str
            The role associated with the requirement.

        Returns
        -------
        dict[str, Any]
            A dictionary representing the missing requirement row.
        """
        row: dict[str, Any] = {
            'connector': connector,
            'extra': requirement.extra or '',
            'guidance': cls.missing_requirement_guidance(
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

    @classmethod
    def strict_config_issue_rows(
        cls,
        *,
        raw: StrAnyMap,
    ) -> list[dict[str, Any]]:
        """
        Return strict-mode config issues hidden by tolerant parsing.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries representing the strict-mode config issues.
        """
        return _strict.strict_config_issue_rows(
            raw=raw,
            connector_type_guidance=cls.connector_type_guidance,
            connector_type_choices=cls.connector_type_choices,
        )

    @classmethod
    def strict_connector_names(
        cls,
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
    ) -> set[str] | None:
        """
        Validate connector entries in *section* and return known names.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any issues found.

        Returns
        -------
        set[str] | None
            A set of known connector names, or None if validation fails.
        """
        return _strict.strict_connector_names(
            raw=raw,
            section=section,
            issues=issues,
            connector_type_guidance=cls.connector_type_guidance,
            connector_type_choices=cls.connector_type_choices,
        )

    @classmethod
    def strict_job_issue_rows(
        cls,
        *,
        raw: StrAnyMap,
        issues: list[dict[str, Any]],
        source_names: set[str] | None,
        target_names: set[str] | None,
        transform_names: set[str] | None,
        validation_names: set[str] | None,
    ) -> None:
        """Append strict-mode job diagnostics to *issues*.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        source_names : set[str] | None
            A set of known source names, or None if validation fails.
        target_names : set[str] | None
            A set of known target names, or None if validation fails.
        transform_names : set[str] | None
            A set of known transform names, or None if validation fails.
        validation_names : set[str] | None
            A set of known validation names, or None if validation fails.
        """
        _strict.strict_job_issue_rows(
            raw=raw,
            issues=issues,
            source_names=source_names,
            target_names=target_names,
            transform_names=transform_names,
            validation_names=validation_names,
        )

    @classmethod
    def strict_job_ref_issue(
        cls,
        *,
        entry: Mapping[str, Any],
        field: str,
        index: int,
        issues: list[dict[str, Any]],
        job_name: str | None,
        required: bool,
        required_key: str,
        section_names: set[str] | None,
        section_label: str,
    ) -> None:
        """Append one strict-mode job reference issue when needed.

        Parameters
        ----------
        entry : Mapping[str, Any]
            The job entry to validate.
        field : str
            The field name to validate.
        index : int
            The index of the job entry.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        job_name : str | None
            The name of the job, or None if not available.
        required : bool
            Whether the field is required.
        required_key : str
            The key that is required.
        section_names : set[str] | None
            A set of known section names, or None if validation fails.
        section_label : str
            The label of the section.
        """
        _strict.strict_job_ref_issue(
            entry=entry,
            field=field,
            index=index,
            issues=issues,
            job_name=job_name,
            required=required,
            required_key=required_key,
            section_names=section_names,
            section_label=section_label,
        )

    @classmethod
    def strict_named_section_names(
        cls,
        *,
        raw: StrAnyMap,
        section: str,
        issues: list[dict[str, Any]],
        guidance: str,
    ) -> set[str] | None:
        """Validate one mapping-like top-level section and return its keys.

        Parameters
        ----------
        raw : StrAnyMap
            The raw configuration data.
        section : str
            The section name to validate.
        issues : list[dict[str, Any]]
            A list to append any issues found.
        guidance : str
            Guidance message for the validation.

        Returns
        -------
        set[str] | None
            A set of known section names, or None if validation fails.
        """
        return _strict.strict_named_section_names(
            raw=raw,
            section=section,
            issues=issues,
            guidance=guidance,
        )

    @classmethod
    def build(
        cls,
        *,
        config_path: str | None = None,
        env: Mapping[str, str] | None = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        """
        Build one runtime readiness report for the current ETLPlus process.

        Parameters
        ----------
        config_path : str | None
            The path to the configuration file, or None if not provided.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.
        strict : bool
            Whether to perform strict validation.

        Returns
        -------
        dict[str, Any]
            A dictionary containing the readiness report.
        """
        checks: list[dict[str, Any]] = [cls.supported_python_check()]
        if config_path:
            try:
                config_kwargs: dict[str, Any] = {'env': env}
                if strict:
                    config_kwargs['strict'] = True
                checks.extend(cls.config_checks(config_path, **config_kwargs))
            except (OSError, TypeError, ValueError) as exc:
                checks.append(
                    cls.make_check(
                        'config-parse',
                        'error',
                        str(exc),
                        path=config_path,
                    ),
                )
        else:
            checks.append(
                cls.make_check(
                    'config-file',
                    'skipped',
                    (
                        'No configuration file provided; only runtime checks '
                        'were performed.'
                    ),
                ),
            )

        return {
            'status': cls.overall_status(checks),
            'etlplus_version': __version__,
            'python_version': cls.python_version(),
            'checks': checks,
        }

    @classmethod
    def config_checks(
        cls,
        config_path: str,
        *,
        env: Mapping[str, str] | None,
        strict: bool = False,
        include_runtime_checks: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Return readiness checks for one pipeline config path.

        Parameters
        ----------
        config_path : str
            The path to the configuration file.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.
        strict : bool
            Whether to perform strict validation.
        include_runtime_checks : bool
            Whether to include runtime checks.

        Returns
        -------
        list[dict[str, Any]]
            A list of readiness checks.
        """
        path = Path(config_path)
        if not path.exists():
            return [
                cls.make_check(
                    'config-file',
                    'error',
                    f'Configuration file does not exist: {path}',
                    path=str(path),
                ),
            ]

        checks: list[dict[str, Any]] = [
            cls.make_check(
                'config-file',
                'ok',
                f'Configuration file exists: {path}',
                path=str(path),
            ),
        ]
        raw = cls.load_raw_config(str(path))
        checks.append(
            cls.make_check(
                'config-parse',
                'ok',
                'Configuration YAML parsed successfully.',
            ),
        )

        context = cls.resolve_config_context(raw, env=env)
        if context.unresolved_tokens:
            checks.append(
                cls.make_check(
                    'config-substitution',
                    'error',
                    'Configuration still contains unresolved substitution tokens.',
                    missing_env=context.unresolved_tokens,
                    references=cls.token_reference_rows(context.resolved_raw),
                    unresolved_tokens=context.unresolved_tokens,
                ),
            )
            return checks

        checks.append(
            cls.make_check(
                'config-substitution',
                'ok',
                'Configuration substitutions resolved successfully.',
            ),
        )
        resolved_cfg = cast(Config, context.resolved_cfg)

        if strict:
            strict_issues = cls.strict_config_issue_rows(raw=context.resolved_raw)
            if strict_issues:
                checks.append(
                    cls.make_check(
                        'config-structure',
                        'error',
                        (
                            'Strict config validation found malformed or inconsistent '
                            'configuration entries.'
                        ),
                        issues=strict_issues,
                    ),
                )
                return checks
            checks.append(
                cls.make_check(
                    'config-structure',
                    'ok',
                    (
                        'Strict config validation found no malformed or '
                        'inconsistent configuration entries.'
                    ),
                ),
            )

        if not include_runtime_checks:
            return checks

        checks.extend(cls.connector_readiness_checks(resolved_cfg))
        checks.extend(
            cls.provider_environment_checks(
                cfg=resolved_cfg,
                env=context.effective_env,
            ),
        )
        return checks

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
    ) -> _ResolvedConfigContext:
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
        _ResolvedConfigContext
            The resolved configuration context.
        """
        cfg = Config.from_dict(raw)
        effective_env = cls.effective_environment(cfg, env)
        resolved = cast(StrAnyMap, deep_substitute(raw, cfg.vars, effective_env))
        unresolved_tokens = sorted(cls.collect_substitution_tokens(resolved))
        return _ResolvedConfigContext(
            raw=raw,
            effective_env=effective_env,
            unresolved_tokens=unresolved_tokens,
            resolved_raw=resolved,
            resolved_cfg=(None if unresolved_tokens else Config.from_dict(resolved)),
        )

    @classmethod
    def strict_config_report(
        cls,
        *,
        config_path: str,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Return one strict config-validation report for the CLI command
        ``etlplus check --strict``.

        Parameters
        ----------
        config_path : str
            The path to the configuration file.
        env : Mapping[str, str] | None
            Environment variables to use for configuration substitution.

        Returns
        -------
        dict[str, Any]
            The strict config-validation report.
        """
        checks = cls.config_checks(
            config_path,
            env=env,
            strict=True,
            include_runtime_checks=False,
        )
        return {
            'status': cls.overall_status(checks),
            'etlplus_version': __version__,
            'checks': checks,
        }

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
        minimum, maximum = _SUPPORTED_PYTHON_RANGE
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
                    for match in _TOKEN_PATTERN.findall(node):
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
