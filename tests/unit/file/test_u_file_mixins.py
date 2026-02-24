"""
:mod:`tests.unit.file.test_u_file_mixins` module.

Unit tests for :mod:`etlplus.file._mixins`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _mixins as mod

# SECTION: HELPERS ========================================================== #


class _PayloadMixin(mod.SemiStructuredPayloadMixin):
    """Concrete payload mixin stub for direct coercion tests."""

    format_name = 'INI'


# SECTION: TESTS ============================================================ #


class TestSemiStructuredPayloadMixin:
    """Unit tests for dict-root coercion behavior."""

    def test_coerce_dict_root_payload_custom_error_message(self) -> None:
        """Test custom dict-root error message override."""
        with pytest.raises(TypeError, match='custom message'):
            _PayloadMixin().coerce_dict_root_payload(
                ['not', 'dict'],
                error_message='custom message',
            )

    def test_coerce_dict_root_payload_default_error_message(self) -> None:
        """Test default dict-root error message path."""
        with pytest.raises(TypeError, match='INI root must be a dict'):
            _PayloadMixin().coerce_dict_root_payload(['not', 'dict'])
