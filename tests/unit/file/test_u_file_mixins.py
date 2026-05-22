"""
:mod:`tests.unit.file.test_u_file_mixins` module.

Unit tests for :mod:`etlplus.file._mixins`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _mixins as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _PayloadMixin(mod.SemiStructuredPayloadMixin):
    """Concrete payload mixin stub for direct coercion tests."""

    format_name = 'INI'


# SECTION: TESTS ============================================================ #


class TestSemiStructuredPayloadMixin:
    """Unit tests for dict-root coercion behavior."""

    @pytest.mark.parametrize(
        ('error_message', 'match'),
        [
            pytest.param(None, 'INI root must be a dict', id='default-message'),
            pytest.param('custom message', 'custom message', id='custom-message'),
        ],
    )
    def test_coerce_dict_root_payload_error_messages(
        self,
        error_message: str | None,
        match: str,
    ) -> None:
        """Test dict-root error message variants."""
        kwargs = {} if error_message is None else {'error_message': error_message}
        with pytest.raises(TypeError, match=match):
            _PayloadMixin().coerce_dict_root_payload(['not', 'dict'], **kwargs)
