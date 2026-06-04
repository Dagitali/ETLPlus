"""
:mod:`tests.unit.telemetry.pytest_telemetry_support` module.

Shared telemetry test doubles for unit tests.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from dataclasses import field
from types import ModuleType

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class FakeMetricRecorder:
    """Minimal OpenTelemetry metric instrument test double."""

    calls: list[tuple[int, dict[str, object]]] = field(default_factory=list)

    def add(self, value: int, attributes: dict[str, object]) -> None:
        """Record one counter increment call."""
        self.calls.append((value, dict(attributes)))

    def record(self, value: int, attributes: dict[str, object]) -> None:
        """Record one histogram sample call."""
        self.calls.append((value, dict(attributes)))


@dataclass
class FakeStatus:
    """Minimal OpenTelemetry status test double."""

    code: object
    description: str | None = None


# SECTION: CLASSES ========================================================== #


class FakeSpan:
    """Minimal OpenTelemetry span test double."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.attributes: dict[str, object] = {}
        self.events: list[tuple[str, dict[str, object]]] = []
        self.exceptions: list[Exception] = []
        self.status: object | None = None
        self.ended = False

    def add_event(
        self,
        name: str,
        attributes: dict[str, object],
    ) -> None:
        """Record one span event."""
        self.events.append((name, dict(attributes)))

    def end(self) -> None:
        """Mark the fake span as ended."""
        self.ended = True

    def record_exception(
        self,
        exc: Exception,
    ) -> None:
        """Record one exception attached to the span."""
        self.exceptions.append(exc)

    def set_attributes(
        self,
        attributes: dict[str, object],
    ) -> None:
        """Merge span attributes into the fake span."""
        self.attributes.update(attributes)

    def set_status(
        self,
        status: object,
    ) -> None:
        """Store one final span status value."""
        self.status = status


class FakeTracer:
    """Minimal OpenTelemetry tracer test double."""

    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    def start_span(
        self,
        name: str,
    ) -> FakeSpan:
        """Create and return one fake span."""
        span = FakeSpan(name)
        self.spans.append(span)
        return span


class FakeMeter:
    """Minimal OpenTelemetry meter test double."""

    def __init__(self) -> None:
        self.counters: list[FakeMetricRecorder] = []
        self.histograms: list[FakeMetricRecorder] = []

    def create_counter(self, *_args: object, **_kwargs: object) -> FakeMetricRecorder:
        """Create and return one fake counter."""
        counter = FakeMetricRecorder()
        self.counters.append(counter)
        return counter

    def create_histogram(
        self,
        *_args: object,
        **_kwargs: object,
    ) -> FakeMetricRecorder:
        """Create and return one fake histogram."""
        histogram = FakeMetricRecorder()
        self.histograms.append(histogram)
        return histogram


class FakeStatusCode:
    """Minimal OpenTelemetry status-code namespace."""

    OK = 'ok'
    ERROR = 'error'


# SECTION: FUNCTIONS ======================================================== #


def install_fake_opentelemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FakeTracer, FakeMeter]:
    """Install lightweight fake OpenTelemetry modules into ``sys.modules``."""
    tracer = FakeTracer()
    meter = FakeMeter()

    trace_mod = ModuleType('opentelemetry.trace')
    # type: ignore[attr-defined]
    trace_mod.get_tracer = lambda *_args, **_kwargs: tracer
    trace_mod.Status = FakeStatus  # type: ignore[attr-defined]  # noqa: N815
    trace_mod.StatusCode = FakeStatusCode  # type: ignore[attr-defined]  # noqa: N815

    metrics_mod = ModuleType('opentelemetry.metrics')
    # type: ignore[attr-defined]
    metrics_mod.get_meter = lambda *_args, **_kwargs: meter

    root_mod = ModuleType('opentelemetry')
    root_mod.trace = trace_mod  # type: ignore[attr-defined]
    root_mod.metrics = metrics_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, 'opentelemetry', root_mod)
    monkeypatch.setitem(sys.modules, 'opentelemetry.trace', trace_mod)
    monkeypatch.setitem(sys.modules, 'opentelemetry.metrics', metrics_mod)
    return tracer, meter
