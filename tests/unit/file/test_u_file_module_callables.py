"""
:mod:`tests.unit.file.test_u_file_module_callables` module.

Unit tests for :mod:`etlplus.file._module_callables`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import _module_callables as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestModuleCallables:
    """Unit tests for optional module callable helpers."""

    @pytest.mark.parametrize(
        ('module', 'kwargs', 'error_type', 'match'),
        [
            pytest.param(
                object(),
                {
                    'format_name': 'XPT',
                    'method_name': 'read_xpt',
                    'operation': 'read',
                    'module_name': 'pyreadstat',
                },
                ImportError,
                'XPT read support requires "pyreadstat" with read_xpt\\(\\)\\.',
                id='missing-method',
            ),
            pytest.param(
                None,
                {
                    'format_name': 'SAV',
                    'method_name': 'write_sav',
                    'operation': 'write',
                    'module_name': 'pyreadstat',
                },
                RuntimeError,
                'pyreadstat dependency is required for SAV write',
                id='missing-module',
            ),
        ],
    )
    def test_call_module_method_error_cases(
        self,
        module: object | None,
        kwargs: dict[str, str],
        error_type: type[Exception],
        match: str,
    ) -> None:
        """Test required module call errors for missing modules or methods."""
        with pytest.raises(error_type, match=match):
            mod.call_module_method(module, **kwargs)

    def test_call_module_method_success_path(self) -> None:
        """Test that required module method invocation with args and kwargs."""

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
        """
        Test that optional read helper returns None for unsupported methods.
        """
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
        """
        Test that callable resolution returns ``None`` for absent modules.
        """
        assert mod.resolve_module_method(None, 'read_any') is None
