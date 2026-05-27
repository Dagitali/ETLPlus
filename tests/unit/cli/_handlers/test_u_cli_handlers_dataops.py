"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_dataops` module.

Unit tests for CLI handler implementation modules.
"""

from __future__ import annotations

import pytest

from etlplus.cli._handlers import dataops as dataops_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


POLICY = dataops_mod.DataCommandPolicy


# SECTION: TESTS ============================================================ #


class TestDataops:
    """Unit tests for internal helpers in :mod:`etlplus.cli._handlers.dataops`."""

    @pytest.mark.parametrize(
        ('target', 'expected'),
        [
            (None, False),
            ('-', False),
            (' - ', False),
            ('   ', False),
            ('out.json', True),
        ],
    )
    def test_has_named_target(
        self,
        target: str | None,
        expected: bool,
    ) -> None:
        """Concrete-target detection should exclude STDOUT-style targets."""
        assert POLICY.has_named_target(target) is expected

    def test_resolve_source_mapping_inputs_uses_shared_source_resolution(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mapping-input resolution should share explicit-format state."""
        source_calls: list[tuple[str, str | None, bool, bool]] = []
        mapping_calls: list[tuple[object, bool, str]] = []

        def fake_source(
            source: object,
            *,
            format_hint: str | None,
            format_explicit: bool,
            hydrate_files: bool = True,
        ) -> object:
            source_calls.append(
                (str(source), format_hint, format_explicit, hydrate_files),
            )
            return {'source': source}

        def fake_mapping(
            payload: object,
            *,
            format_explicit: bool,
            error_message: str,
        ) -> dict[str, object]:
            mapping_calls.append((payload, format_explicit, error_message))
            return {'select': ['id']}

        monkeypatch.setattr(dataops_mod._input, 'resolve_cli_payload', fake_source)
        monkeypatch.setattr(
            dataops_mod._payload,
            'resolve_mapping_payload',
            fake_mapping,
        )

        payload, mapping = POLICY.resolve_source_mapping_inputs(
            source='data.json',
            mapping_payload='ops.json',
            source_format='json',
            format_explicit=False,
            error_message='bad mapping',
        )

        assert payload == {'source': 'data.json'}
        assert mapping == {'select': ['id']}
        assert source_calls == [('data.json', 'json', True, True)]
        assert mapping_calls == [('ops.json', True, 'bad mapping')]
