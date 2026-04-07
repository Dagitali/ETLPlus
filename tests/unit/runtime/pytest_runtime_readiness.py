"""
:mod:`tests.unit.runtime.pytest_runtime_readiness` module.

Shared helpers for pytest-based runtime readiness tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime._readiness as readiness_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FUNCTIONS ======================================================== #


def build_runtime_cfg(
    *,
    sources: list[object] | None = None,
    targets: list[object] | None = None,
    apis: dict[str, object] | None = None,
    profile_env: dict[str, str] | None = None,
    variables: dict[str, object] | None = None,
) -> Any:
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
    row: dict[str, object] = {
        'connector': connector,
        'issue': issue,
        'role': role,
    }
    if connector_type is not None:
        row['type'] = connector_type
    if guidance is not None:
        row['guidance'] = guidance
    if supported_types is not None:
        row['supported_types'] = supported_types
    return row


def build_issue_row(**fields: object) -> dict[str, object]:
    """Build one expected strict-validation issue row."""
    return dict(fields)


def build_missing_requirement_row(
    *,
    connector: str,
    missing_package: str,
    reason: str,
    role: str,
    extra: str,
    guidance: str | None = None,
    detected_format: str | None = None,
    detected_scheme: str | None = None,
) -> dict[str, object]:
    """Build one missing-optional-dependency row."""
    row: dict[str, object] = {
        'connector': connector,
        'extra': extra,
        'missing_package': missing_package,
        'reason': reason,
        'role': role,
    }
    if guidance is not None:
        row['guidance'] = guidance
    if detected_format is not None:
        row['detected_format'] = detected_format
    if detected_scheme is not None:
        row['detected_scheme'] = detected_scheme
    return row


def build_provider_check(
    *,
    status: str,
    message: str,
    rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """Build one provider-environment check row."""
    row: dict[str, object] = {
        'message': message,
        'name': 'provider-environment',
        'status': status,
    }
    if rows is not None:
        row['environment_gaps'] = rows
    return row


def build_provider_gap_row(**fields: object) -> dict[str, object]:
    """Build one provider-environment gap row for wrapper-level tests."""
    return dict(fields)


def build_resolved_config_context(
    raw: Mapping[str, object],
    *,
    env: Mapping[str, str] | None = None,
    unresolved_tokens: list[str] | None = None,
    resolved_raw: Mapping[str, object] | None = None,
    resolved_cfg: object | None = None,
) -> readiness_mod._ResolvedConfigContext:
    """Build one resolved-config context with stable defaults."""
    return readiness_mod._ResolvedConfigContext(
        raw=raw,
        effective_env={} if env is None else dict(env),
        unresolved_tokens=[] if unresolved_tokens is None else list(unresolved_tokens),
        resolved_raw=raw if resolved_raw is None else dict(resolved_raw),
        resolved_cfg=cast(
            Any,
            build_runtime_cfg() if resolved_cfg is None else resolved_cfg,
        ),
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
        readiness_mod.ReadinessReportBuilder,
        'load_raw_config',
        lambda _path: raw_config,
    )
    monkeypatch.setattr(
        readiness_mod.ReadinessReportBuilder,
        'resolve_config_context',
        lambda raw, env=None: build_resolved_config_context(
            cast(Mapping[str, object], raw),
            env=env,
            unresolved_tokens=unresolved_tokens,
            resolved_raw=effective_resolved_raw,
            resolved_cfg=resolved_cfg,
        ),
    )


def patch_file_read(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    """Patch :class:`File` to return one fixed payload from :meth:`File.read`."""

    class _FakeFile:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def read(self) -> object:
            return payload

    monkeypatch.setattr(readiness_mod, 'File', _FakeFile)


def write_pipeline_config(
    tmp_path: Path,
    *,
    contents: str = 'name: pipeline\n',
) -> Path:
    """Write one minimal pipeline config and return its path."""
    config_path = tmp_path / 'pipeline.yml'
    config_path.write_text(contents, encoding='utf-8')
    return config_path
