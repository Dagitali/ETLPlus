"""
tests.integration.conftest pytest configuration module.

Configures pytest-based integration tests.
"""
import pytest

# Mark all tests in this directory as integration tests
pytestmark = pytest.mark.integration
