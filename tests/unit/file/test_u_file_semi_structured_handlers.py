"""
:mod:`tests.unit.file.test_u_file_semi_structured_handlers` module.

Unit tests for :mod:`etlplus.file._semi_structured_handlers`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _semi_structured_handlers as mod
from etlplus.file._enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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

    @pytest.mark.parametrize(
        ('handler_cls', 'match'),
        [
            (
                _BadRootDefaultHandler,
                'INI root must be a dict',
            ),
            (
                _BadRootOverrideHandler,
                'override root error',
            ),
        ],
    )
    def test_loads_raises_type_error_for_non_dict_root(
        self,
        handler_cls: type[_BadRootDefaultHandler],
        match: str,
    ) -> None:
        """Test dict-root error message variants."""
        with pytest.raises(TypeError, match=match):
            handler_cls().loads('payload')
