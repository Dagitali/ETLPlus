"""
:mod:`tests.unit.file.test_u_file_jinja2` module.

Unit tests for :mod:`etlplus.file.jinja2`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from etlplus.file import jinja2 as mod
from etlplus.file.base import ReadOptions

from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripUnitModuleContract

# SECTION: TESTS ============================================================ #


@dataclass
class _TemplateStub:
    """Minimal Jinja2 Template substitute."""

    template: str
    render_calls: list[dict[str, Any]]

    def render(self, **context: Any) -> str:
        """Capture context and return deterministic output."""
        self.render_calls.append(context)
        return f'{self.template}|{context}'


class _Jinja2Stub:
    """Minimal module stub exposing ``Template``."""

    def __init__(self) -> None:
        self.template_calls: list[str] = []
        self.template_instances: list[_TemplateStub] = []

    def Template(self, template: str) -> _TemplateStub:  # noqa: N802
        """Build one template stub instance and record input."""
        self.template_calls.append(template)
        stub = _TemplateStub(template=template, render_calls=[])
        self.template_instances.append(stub)
        return stub


class TestJinja2(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.jinja2`."""

    module = mod
    format_name = 'jinja2'
    roundtrip_spec = RoundtripSpec(
        payload={'template': 'Hello {{ name }}'},
        expected=[{'template': 'Hello {{ name }}'}],
    )

    def test_read_honors_encoding_options(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reads honoring explicit text encoding options."""
        path = self.format_path(tmp_path, stem='latin1')
        path.write_bytes('Olá {{ name }}'.encode('latin-1'))

        result = self.module_handler.read(
            path,
            options=ReadOptions(encoding='latin-1'),
        )

        assert result == [{'template': 'Olá {{ name }}'}]

    def test_read_returns_template_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reads returning one-row payload with template text."""
        path = self.format_path(tmp_path)
        path.write_text('Hello {{ name }}', encoding='utf-8')

        assert self.module_handler.read(path) == [
            {'template': 'Hello {{ name }}'},
        ]

    def test_render_uses_optional_jinja2_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test render delegating to optional Jinja2 dependency."""
        jinja2_stub = _Jinja2Stub()
        monkeypatch.setattr(
            mod,
            'get_dependency',
            lambda *args, **kwargs: jinja2_stub,  # noqa: ARG005
        )

        result = self.module_handler.render(
            'Hello {{ name }}',
            {'name': 'Ada'},
        )

        assert result == "Hello {{ name }}|{'name': 'Ada'}"
        assert jinja2_stub.template_calls == ['Hello {{ name }}']
        assert jinja2_stub.template_instances
        assert jinja2_stub.template_instances[0].render_calls == [
            {'name': 'Ada'},
        ]

    def test_write_requires_single_template_object(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes requiring exactly one object with template string."""
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match='exactly one object'):
            self.module_handler.write(
                path,
                [{'template': 'a'}, {'template': 'b'}],
            )

        with pytest.raises(TypeError, match='"template" string'):
            self.module_handler.write(path, [{'name': 'missing'}])

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty write payload returning zero without creating file."""
        path = self.format_path(tmp_path)

        assert self.module_handler.write(path, []) == 0
        assert not path.exists()
