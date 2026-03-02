"""
:mod:`tests.unit.file.test_u_file_template_handlers` module.

Unit tests for :mod:`etlplus.file._template_handlers`.
"""

from __future__ import annotations

from etlplus.file import _template_handlers as mod
from etlplus.file.enums import FileFormat

# SECTION: HELPERS ========================================================== #


class _TemplateHandler(mod.BraceTokenTemplateHandlerMixin):
    """Concrete brace-token template handler for mixin tests."""

    format = FileFormat.MUSTACHE
    template_engine = 'mustache'


# SECTION: TESTS ============================================================ #


class TestTemplateHandlers:
    """Unit tests for shared template-handler mixins."""

    def test_brace_token_pattern_extracts_context_key(self) -> None:
        """Test brace token regex extracting the ``key`` group."""
        match = _TemplateHandler.token_pattern.search('{{ user_name }}')
        assert match is not None
        assert match.group('key') == 'user_name'

    def test_render_substitutes_brace_tokens(self) -> None:
        """Test brace-token render path replacing keys with context values."""
        handler = _TemplateHandler()
        result = handler.render('Hello {{name}}!', {'name': 'Ada'})
        assert result == 'Hello Ada!'

    def test_render_missing_key_defaults_to_empty_string(self) -> None:
        """Test brace-token render path using empty string for missing keys."""
        handler = _TemplateHandler()
        result = handler.render('Hello {{missing}}!', {})
        assert result == 'Hello !'
