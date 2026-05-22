"""
:mod:`tests.unit.runtime.pytest_runtime_readiness` module.

Shared helpers for pytest-based runtime readiness tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import etlplus.runtime.readiness._builder as readiness_builder_mod
from etlplus.runtime.readiness._support import ResolvedConfigContext

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _row(
    fields: Mapping[str, object],
    *,
    optional_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return one test row with optional non-``None`` fields attached."""
    if optional_fields:
        return dict(fields) | {
            key: value for key, value in optional_fields.items() if value is not None
        }
    return dict(fields)


# SECTION: FUNCTIONS ======================================================== #


def build_runtime_cfg(
    *,
    sources: list[object] | None = None,
    targets: list[object] | None = None,
    apis: dict[str, object] | None = None,
    profile_env: dict[str, str] | None = None,
    variables: dict[str, object] | None = None,
) -> SimpleNamespace:
    """Build one light-weight config-like object for readiness tests."""
    return SimpleNamespace(
        apis={} if apis is None else dict(apis),
        profile=SimpleNamespace(env={} if profile_env is None else dict(profile_env)),
        sources=[] if sources is None else list(sources),
        targets=[] if targets is None else list(targets),
        vars={} if variables is None else dict(variables),
    )


def build_connector_gap_row(
    *,
    connector: str,
    issue: str,
    role: str,
    connector_type: str | None = None,
    guidance: str | None = None,
    supported_types: list[str] | None = None,
) -> dict[str, object]:
    """Build one connector-gap row for readiness assertions."""
    return _row(
        {
            'connector': connector,
            'issue': issue,
            'role': role,
        },
        optional_fields={
            'guidance': guidance,
            'supported_types': supported_types,
            'type': connector_type,
        },
    )


def build_issue_row(**fields: object) -> dict[str, object]:
    """Build one expected strict-validation issue row."""
    return _row(fields)


def build_missing_requirement_row(
    *,
    connector: str,
    detected_database_provider: str | None = None,
    missing_package: str,
    reason: str,
    role: str,
    extra: str,
    guidance: str | None = None,
    detected_format: str | None = None,
    detected_queue_service: str | None = None,
    detected_scheme: str | None = None,
) -> dict[str, object]:
    """Build one missing-optional-dependency row."""
    return _row(
        {
            'connector': connector,
            'extra': extra,
            'missing_package': missing_package,
            'reason': reason,
            'role': role,
        },
        optional_fields={
            'detected_database_provider': detected_database_provider,
            'detected_format': detected_format,
            'detected_queue_service': detected_queue_service,
            'detected_scheme': detected_scheme,
            'guidance': guidance,
        },
    )


def build_provider_check(
    *,
    status: str,
    message: str,
    rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build one provider-environment check row."""
    return _row(
        {
            'message': message,
            'name': 'provider-environment',
            'status': status,
        },
        optional_fields={'environment_gaps': rows},
    )


def build_provider_gap_row(**fields: object) -> dict[str, object]:
    """Build one provider-environment gap row for wrapper-level tests."""
    return _row(fields)


def build_resolved_config_context(
    raw: Mapping[str, object],
    *,
    env: Mapping[str, str] | None = None,
    unresolved_tokens: list[str] | None = None,
    resolved_raw: Mapping[str, object] | None = None,
    resolved_cfg: object | None = None,
) -> ResolvedConfigContext:
    """Build one resolved-config context with stable defaults."""
    return ResolvedConfigContext(
        raw=raw,
        effective_env={} if env is None else dict(env),
        unresolved_tokens=[] if unresolved_tokens is None else list(unresolved_tokens),
        resolved_raw=raw if resolved_raw is None else dict(resolved_raw),
        resolved_cfg=build_runtime_cfg() if resolved_cfg is None else resolved_cfg,
    )


def build_readiness_check(
    *,
    name: str,
    status: str,
    message: str,
    details_key: str | None = None,
    rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build one generic readiness check row for connector/provider tests."""
    return _row(
        {
            'message': message,
            'name': name,
            'status': status,
        },
        optional_fields={details_key: rows} if details_key is not None else None,
    )


def patch_config_resolution(
    monkeypatch: pytest.MonkeyPatch,
    *,
    raw: Mapping[str, object],
    resolved_cfg: object | None = None,
    unresolved_tokens: list[str] | None = None,
    resolved_raw: Mapping[str, object] | None = None,
) -> None:
    """Patch raw-config loading and context resolution for one test scenario."""
    raw_config = dict(raw)
    effective_resolved_raw = raw_config if resolved_raw is None else dict(resolved_raw)

    monkeypatch.setattr(
        readiness_builder_mod.ReadinessReportBuilder,
        'load_raw_config',
        lambda _path: raw_config,
    )
    monkeypatch.setattr(
        readiness_builder_mod.ReadinessReportBuilder,
        'resolve_config_context',
        lambda raw, env=None: build_resolved_config_context(
            cast(Mapping[str, object], raw),
            env=env,
            unresolved_tokens=unresolved_tokens,
            resolved_raw=effective_resolved_raw,
            resolved_cfg=resolved_cfg,
        ),
    )


def write_pipeline_config(
    tmp_path: Path,
    *,
    contents: str = 'name: pipeline\n',
) -> Path:
    """Write one minimal pipeline config and return its path."""
    config_path = tmp_path / 'pipeline.yml'
    config_path.write_text(contents, encoding='utf-8')
    return config_path
