"""
:mod:`tests.e2e` package.

End-to-end (E2E) tests for the :mod:`etlplus` package.

Notes
-----
- What they test: A complete, user-visible workflow across the system boundary,
    typically exercising a real scenario end-to-end (e.g., ingest → process →
    persist/publish), including orchestration, configuration parsing, component
    wiring, and side-effects on external state.
- Dependencies: Real dependencies (filesystem, database, HTTP server, queues)
    or realistic test doubles that behave like real services (e.g., Dockerized
    databases, a local test HTTP server, service emulators). These tests
    generally avoid mocks except for hard-to-control externalities (e.g.,
    time).
- Goal: Provide confidence that the application behaves correctly as a whole in
    a production-like setup—catching boundary and wiring failures that narrower
    tests often miss (serialization, auth, connection strings, retries/backoff,
    pagination/rate-limiting interactions, ordering, idempotency, and
    persistence semantics).
"""

from __future__ import annotations
