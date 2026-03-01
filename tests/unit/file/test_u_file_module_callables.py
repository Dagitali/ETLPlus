"""
:mod:`tests.unit.file.test_u_file_module_callables` module.

Unit tests for :mod:`etlplus.file._module_callables`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import _module_callables as mod

# SECTION: TESTS ============================================================ #


class TestModuleCallables:
    """Unit tests for optional module callable helpers."""

    def test_call_module_method_missing_method_uses_custom_module_name(
        self,
    ) -> None:
        """Test missing-method errors including provided module metadata."""
        with pytest.raises(
            ImportError,
            match=(
                'XPT read support requires "pyreadstat" with read_xpt\\(\\)\\.'
            ),
        ):
            mod.call_module_method(
                object(),
                format_name='XPT',
                method_name='read_xpt',
                operation='read',
                module_name='pyreadstat',
            )

    def test_call_module_method_raises_when_dependency_is_missing(
        self,
    ) -> None:
        """Test required module calls raising runtime errors for ``None``."""
        with pytest.raises(
            RuntimeError,
            match='pyreadstat dependency is required for SAV write',
        ):
            mod.call_module_method(
                None,
                format_name='SAV',
                method_name='write_sav',
                operation='write',
                module_name='pyreadstat',
            )

    def test_call_module_method_success_path(self) -> None:
        """Test required module method invocation with args and kwargs."""

        class _Module:
            def read_any(self, path: str, *, limit: int) -> tuple[str, int]:
                """Simulate a module method that accepts args and kwargs."""
                return path, limit

        result = mod.call_module_method(
            _Module(),
            format_name='XPT',
            method_name='read_any',
            operation='read',
            args=('sample.xpt',),
            kwargs={'limit': 3},
        )

        assert result == ('sample.xpt', 3)

    def test_read_module_frame_if_supported_returns_none_when_unsupported(
        self,
    ) -> None:
        """Test optional read helper returning None for unsupported methods."""
        path = Path('sample.sav')
        assert (
            mod.read_module_frame_if_supported(
                object(),
                method_name='read_sav',
                path=path,
            )
            is None
        )

    def test_resolve_module_method_returns_none_for_missing_module(
        self,
    ) -> None:
        """Test callable resolution returning ``None`` for absent modules."""
        assert mod.resolve_module_method(None, 'read_any') is None
