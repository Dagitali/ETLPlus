"""
:mod:`tests.unit.file.test_u_file_semi_structured_handlers` module.

Unit tests for :mod:`etlplus.file._semi_structured_handlers`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _semi_structured_handlers as mod
from etlplus.file.enums import FileFormat

# SECTION: HELPERS ========================================================== #


class _BadRootDefaultHandler(mod.DictPayloadTextCodecHandlerMixin):
    """Dict-codec handler stub using default dict-root error message."""

    format = FileFormat.INI

    def encode_dict_payload_text(
        self,
        payload: dict[str, object],  # noqa: ARG002
    ) -> str:
        return 'encoded'

    def decode_dict_payload_text(self, text: str) -> object:  # noqa: ARG002
        return ['not', 'a', 'dict']


class _BadRootOverrideHandler(_BadRootDefaultHandler):
    """Dict-codec handler stub with explicit dict-root error message."""

    dict_root_error_message = 'override root error'


# SECTION: TESTS ============================================================ #


class TestDictPayloadTextCodecHandlerMixin:
    """Unit tests for dict-root payload coercion branches."""

    def test_loads_raises_default_type_error_for_non_dict_root(self) -> None:
        """Test default dict-root error message branch."""
        with pytest.raises(TypeError, match='INI root must be a dict'):
            _BadRootDefaultHandler().loads('payload')

    def test_loads_raises_override_error_for_non_dict_root(self) -> None:
        """Test override dict-root error message branch."""
        with pytest.raises(TypeError, match='override root error'):
            _BadRootOverrideHandler().loads('payload')
