"""
:mod:`tests.unit.cli._handlers.test_u_cli_handlers_dataops` module.

Unit tests for CLI handler implementation modules.
"""

from __future__ import annotations

from typing import cast

import pytest

from etlplus.cli._handlers import _lifecycle as lifecycle_mod
from etlplus.cli._handlers import dataops as dataops_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


POLICY = dataops_mod.DataCommandPolicy


# SECTION: TESTS ============================================================ #


class TestDataops:
    """Unit tests for internal helpers in :mod:`etlplus.cli._handlers.dataops`."""

    def test_complete_json_success_uses_json_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """JSON success helper should delegate through the shared completer."""
        captured: dict[
            str,
            tuple[
                lifecycle_mod.CommandContext,
                object,
                str,
                bool,
                dict[str, object],
            ],
        ] = {}

        def fake_complete(
            context: lifecycle_mod.CommandContext,
            payload: object,
            *,
            mode: str,
            pretty: bool = True,
            **fields: object,
        ) -> int:
            captured['params'] = (context, payload, mode, pretty, fields)
            return 9

        monkeypatch.setattr(POLICY, 'complete_success', fake_complete)

        result = POLICY.complete_json_success(
            cast(lifecycle_mod.CommandContext, object()),
            {'ok': True},
            pretty=False,
            source='data.json',
        )

        assert result == 9
        _, payload, mode, pretty, fields = captured['params']
        assert payload == {'ok': True}
        assert mode == 'json'
        assert pretty is False
        assert fields == {'source': 'data.json'}

    @pytest.mark.parametrize(
        ('target', 'expected'),
        [
            (None, 'stdout'),
            ('-', 'stdout'),
            ('out.json', 'out.json'),
        ],
    )
    def test_display_target(
        self,
        target: str | None,
        expected: str,
    ) -> None:
        """Target display labels should normalize STDOUT-style targets."""
        assert POLICY.display_target(target) == expected

    @pytest.mark.parametrize(
        ('target', 'expected'),
        [
            (None, False),
            ('-', False),
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

    @pytest.mark.parametrize(
        ('format_hint', 'explicit', 'expected'),
        [
            (None, False, False),
            ('json', False, True),
            (None, True, True),
        ],
    )
    def test_is_explicit_format(
        self,
        format_hint: str | None,
        explicit: bool,
        expected: bool,
    ) -> None:
        """Explicit-format detection should honor hints and override flags."""
        assert (
            POLICY.is_explicit_format(
                format_hint=format_hint,
                explicit=explicit,
            )
            is expected
        )

    @pytest.mark.parametrize(
        ('result', 'expected'),
        [
            ({'status': 'queued'}, 'queued'),
            ({'status': 1}, 'ok'),
            ('not-a-mapping', 'ok'),
        ],
    )
    def test_result_status(
        self,
        result: object,
        expected: str,
    ) -> None:
        """Result-status extraction should degrade cleanly for bad payloads."""
        assert POLICY.result_status(result) == expected

    def test_resolve_source_payload_forwards_resolution_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Source payload resolution should delegate with the expected flags."""
        captured: dict[str, object] = {}

        def fake_resolve(
            payload: object,
            *,
            format_hint: str | None,
            format_explicit: bool,
            hydrate_files: bool = True,
        ) -> object:
            captured['params'] = (
                payload,
                format_hint,
                format_explicit,
                hydrate_files,
            )
            return {'payload': payload}

        monkeypatch.setattr(dataops_mod._payload, 'resolve_payload', fake_resolve)

        result = POLICY.resolve_source_payload(
            'data.json',
            source_format='json',
            format_explicit=True,
            hydrate_files=False,
        )

        assert result == {'payload': 'data.json'}
        assert captured['params'] == ('data.json', 'json', True, False)

    def test_resolve_source_mapping_inputs_uses_shared_source_resolution(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mapping-input resolution should share explicit-format state."""
        source_calls: list[tuple[str, str | None, bool, bool]] = []
        mapping_calls: list[tuple[object, bool, str]] = []

        def fake_source(
            source: str,
            *,
            source_format: str | None,
            format_explicit: bool,
            hydrate_files: bool = True,
        ) -> object:
            source_calls.append(
                (source, source_format, format_explicit, hydrate_files),
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

        monkeypatch.setattr(POLICY, 'resolve_source_payload', fake_source)
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
