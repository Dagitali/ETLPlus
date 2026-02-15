"""
:mod:`tests.integration` package.

Integration tests for the :mod:`etlplus` package.

Notes
-----
- What they test: Multiple components working together using real dependencies
    (or “realistic” ones like Dockerized Postgres, LocalStack, a test HTTP
    server).
- Dependencies: Real database/filesystem/network/services, or near-real.
- Goal: Catch wiring/compat issues: serialization, auth, connection strings,
    SQL dialect quirks, filesystem semantics, concurrency issues.
"""

from __future__ import annotations
