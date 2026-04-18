"""
:mod:`tests.unit.cli.pytest_cli_handlers_support` module.

Shared helper seams for CLI handler package unit tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest

from etlplus import Config
from etlplus.cli._handlers import _completion as completion_mod
from etlplus.cli._handlers import _input as input_mod
from etlplus.cli._handlers import _lifecycle as lifecycle_mod
from etlplus.cli._handlers import _output as output_mod
from etlplus.cli._handlers import _summary as summary_mod
from etlplus.cli._handlers import check as check_mod
from etlplus.cli._handlers import dataops as dataops_mod
from etlplus.cli._handlers import init as init_mod
from etlplus.cli._handlers import render as render_mod
from etlplus.cli._handlers import run as run_mod
from etlplus.file import File
from etlplus.history import HistoryStore
from etlplus.history import RunCompletion
from etlplus.history import RunState
from etlplus.runtime import ReadinessReportBuilder
from etlplus.runtime import RuntimeEvents

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


handlers: Any = SimpleNamespace(
    Config=Config,
    File=File,
    HistoryStore=HistoryStore,
    ReadinessReportBuilder=ReadinessReportBuilder,
    RunCompletion=RunCompletion,
    RunState=RunState,
    RuntimeEvents=RuntimeEvents,
    _CommandContext=lifecycle_mod.CommandContext,
    _check_sections=summary_mod.check_sections,
    _complete_output=completion_mod.complete_output,
    _failure_boundary=lifecycle_mod.failure_boundary,
    _input=input_mod,
    _output=output_mod,
    _pipeline_summary=summary_mod.pipeline_summary,
    _summary=summary_mod,
    check_handler=check_mod.check_handler,
    extract_handler=dataops_mod.extract_handler,
    init_handler=init_mod.init_handler,
    load_handler=dataops_mod.load_handler,
    render_handler=render_mod.render_handler,
    run_handler=run_mod.run_handler,
    transform_handler=dataops_mod.transform_handler,
    validate_handler=dataops_mod.validate_handler,
)


# SECTION: TYPE ALIASES ===================================================== #


type ResolveCliPayloadCall = tuple[object, str | None, bool]


# SECTION: FUNCTIONS ======================================================== #


def capture_file_write(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, tuple[str, object]]:
    """Patch :meth:`File.write` and capture the written path plus payload."""
    captured: dict[str, tuple[str, object]] = {}

    def _write(self: File, data: object, **kwargs: object) -> None:
        _ = kwargs
        captured['params'] = (str(self.path), data)

    monkeypatch.setattr(handlers.File, 'write', _write)
    return captured


def patch_config_from_yaml(
    monkeypatch: pytest.MonkeyPatch,
    config: Config,
    *,
    calls: list[tuple[str, bool]] | None = None,
) -> None:
    """Patch :meth:`Config.from_yaml` to return one fixed config object."""

    def _from_yaml(path: str, substitute: bool) -> Config:
        if calls is not None:
            calls.append((path, substitute))
        return config

    monkeypatch.setattr(
        handlers.Config,
        'from_yaml',
        _from_yaml,
    )


def patch_resolve_cli_payload_map(
    monkeypatch: pytest.MonkeyPatch,
    payloads: Mapping[object, object],
    *,
    calls: list[ResolveCliPayloadCall] | None = None,
) -> None:
    """Patch :func:`resolve_cli_payload` with a fixed source-to-payload map."""

    def _resolve(
        source: object,
        *,
        format_hint: str | None,
        format_explicit: bool,
    ) -> object:
        if calls is not None:
            calls.append((source, format_hint, format_explicit))
        return payloads[source]

    monkeypatch.setattr(handlers._input, 'resolve_cli_payload', _resolve)


def transform_payload_map() -> dict[object, object]:
    """Build the default payload map used by transform handler tests."""
    return {
        'data.json': {'source': 'data.json'},
        'ops.json': {'select': ['id']},
    }


def validation_payload_map() -> dict[object, object]:
    """Build the default payload map used by validate handler tests."""
    return {
        'data.json': {'source': 'data.json'},
        'rules.json': {'id': {'required': True}},
    }
