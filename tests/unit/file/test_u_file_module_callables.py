"""
:mod:`tests.unit.file.test_u_file_module_callables` module.

Unit tests for :mod:`etlplus.file._module_callables`.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import pytest

from etlplus.file import _module_callables as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestModuleCallables:
    """Unit tests for optional module callable helpers."""

    @pytest.mark.parametrize(
        (
            'module',
            'format_name',
            'method_name',
            'operation',
            'module_name',
            'error_type',
            'match',
        ),
        [
            (
                object(),
                'XPT',
                'read_xpt',
                'read',
                'pyreadstat',
                ImportError,
                'XPT read support requires "pyreadstat" with read_xpt\\(\\)\\.',
            ),
            (
                None,
                'SAV',
                'write_sav',
                'write',
                'pyreadstat',
                RuntimeError,
                'pyreadstat dependency is required for SAV write',
            ),
        ],
    )
    def test_call_module_method_error_cases(
        self,
        module: object | None,
        format_name: str,
        method_name: str,
        operation: Literal['read', 'write'],
        module_name: str,
        error_type: type[Exception],
        match: str,
    ) -> None:
        """Test required module call errors for missing modules or methods."""
        with pytest.raises(error_type, match=match):
            mod.call_module_method(
                module,
                format_name=format_name,
                method_name=method_name,
                operation=operation,
                module_name=module_name,
            )

    def test_call_module_method_success_path(self) -> None:
        """Test that required module method invocation with args and kwargs."""
        result = mod.call_module_method(
            SimpleNamespace(read_any=lambda path, *, limit: (path, limit)),
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
