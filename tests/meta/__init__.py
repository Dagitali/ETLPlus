"""
:mod:`tests.meta` package.

Meta-level guardrail tests for the :mod:`etlplus` repository.

Notes
-----
- What they test: Repository-wide conventions, documented contracts, stable
    public surface expectations, and test-suite structure rules.
- Dependencies: No real external services; these tests inspect source files,
    docs, registry metadata, and test layout.
- Goal: Fast failure when repository policy, contributor guidance, or stable
    compatibility contracts drift unintentionally.
"""

from __future__ import annotations
