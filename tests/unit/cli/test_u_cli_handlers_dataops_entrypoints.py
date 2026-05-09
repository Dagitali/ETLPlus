"""
:mod:`tests.unit.cli.test_u_cli_handlers_dataops_entrypoints` module.

Unit tests for extract/load/transform/validate entry points in
:mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import ANY

import pytest

from etlplus.cli._handlers import dataops as dataops_mod

from .conftest import CaptureIo
from .conftest import assert_emit_json
from .conftest import assert_emit_or_write
from .pytest_cli_handlers_support import ResolveCliPayloadCall
from .pytest_cli_handlers_support import capture_file_write
from .pytest_cli_handlers_support import handlers
from .pytest_cli_handlers_support import patch_resolve_cli_payload_map
from .pytest_cli_handlers_support import transform_payload_map
from .pytest_cli_handlers_support import validation_payload_map

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestExtractHandler:
    """Unit tests for :func:`extract_handler`."""

    @pytest.mark.parametrize(
        ('target', 'pretty'),
        [
            pytest.param(None, True, id='stdout'),
            pytest.param('export.json', True, id='target-file'),
        ],
    )
    def test_extracts_non_stdin_sources_and_emits_or_writes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
        target: str | None,
        pretty: bool,
    ) -> None:
        """
        Test that :func:`extract_handler` routes non-STDIN sources through
        :func:`extract` and emits or writes the result.
        """
        observed: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            observed['params'] = (source_type, source, file_format)
            return {'status': 'ok'}

        monkeypatch.setattr(dataops_mod, 'extract', fake_extract)

        assert (
            handlers.extract_handler(
                source_type='api',
                source='endpoint',
                source_format='json',
                target=target,
                format_explicit=True,
                pretty=pretty,
            )
            == 0
        )

        assert observed['params'] == ('api', 'endpoint', 'json')
        kwargs = assert_emit_or_write(
            capture_io,
            {'status': 'ok'},
            target,
            pretty=pretty,
        )
        assert kwargs['success_message'] == ANY

    def test_file_respects_explicit_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` forwards explicit file format hints.
        """
        captured: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            captured['params'] = (source_type, source, file_format)
            return {'ok': True}

        monkeypatch.setattr(dataops_mod, 'extract', fake_extract)
        assert (
            handlers.extract_handler(
                source_type='file',
                source='table.dat',
                source_format='csv',
                format_explicit=True,
                output=None,
                pretty=True,
            )
            == 0
        )
        assert captured['params'] == ('file', 'table.dat', 'csv')
        assert len(capture_io['emit_or_write']) == 1

    def test_reads_stdin_and_emits_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` reads STDIN and emits parsed data.
        """
        monkeypatch.setattr(
            handlers._input,
            'read_stdin_text',
            lambda: 'raw-text',
        )
        monkeypatch.setattr(
            handlers._input,
            'parse_text_payload',
            lambda text, fmt_hint: {'payload': text, 'fmt_hint': fmt_hint},
        )

        def fail_extract(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('extract should not be called')

        monkeypatch.setattr(dataops_mod, 'extract', fail_extract)
        assert (
            handlers.extract_handler(
                source_type='api',
                source='-',
                source_format=None,
                format_explicit=False,
                output=None,
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {'payload': 'raw-text', 'fmt_hint': None},
            pretty=False,
        )
        assert capture_io['emit_or_write'] == []

    def test_target_argument_overrides_output_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that ``target`` takes precedence over ``output`` when both are
        set.
        """
        monkeypatch.setattr(
            dataops_mod,
            'extract',
            lambda *_args, **_kwargs: {'status': 'ok'},
        )

        assert (
            handlers.extract_handler(
                source_type='file',
                source='data.json',
                target='preferred.json',
                output='ignored.json',
                source_format='json',
                format_explicit=True,
                pretty=False,
            )
            == 0
        )
        assert_emit_or_write(
            capture_io,
            {'status': 'ok'},
            'preferred.json',
            pretty=False,
        )


class TestLoadHandler:
    """Unit tests for :func:`load_handler`."""

    def test_file_target_streams_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`load_handler` streams payload for file targets."""
        recorded: dict[str, object] = {}

        def fake_materialize(
            src: str,
            *,
            format_hint: str | None,
            format_explicit: bool,
        ) -> list[object]:
            recorded['call'] = (src, format_hint, format_explicit)
            return ['rows', src]

        monkeypatch.setattr(
            dataops_mod,
            'materialize_file_payload',
            fake_materialize,
        )

        def fail_load(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('load should not be called for STDOUT path')

        monkeypatch.setattr(dataops_mod, 'load', fail_load)

        assert (
            handlers.load_handler(
                source='data.csv',
                target_type='file',
                target='-',
                source_format=None,
                target_format=None,
                format_explicit=False,
                output=None,
                pretty=True,
            )
            == 0
        )
        assert recorded['call'] == ('data.csv', None, False)
        assert_emit_json(capture_io, ['rows', 'data.csv'], pretty=True)

    def test_reads_stdin_and_invokes_load(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`load_handler` parses STDIN and routes through load.
        """
        read_calls = {'count': 0}

        def fake_read_stdin() -> str:
            read_calls['count'] += 1
            return 'stdin-payload'

        monkeypatch.setattr(
            handlers._input,
            'read_stdin_text',
            fake_read_stdin,
        )

        parsed_payload = {'payload': 'stdin-payload', 'fmt_hint': None}
        parse_calls: dict[str, object] = {}

        def fake_parse(text: str, fmt_hint: str | None) -> object:
            parse_calls['params'] = (text, fmt_hint)
            return parsed_payload

        monkeypatch.setattr(handlers._input, 'parse_text_payload', fake_parse)

        def fail_materialize(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                'materialize_file_payload should not be called for STDIN sources',
            )

        monkeypatch.setattr(
            dataops_mod,
            'materialize_file_payload',
            fail_materialize,
        )

        load_record: dict[str, object] = {}

        def fake_load(
            payload: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (payload, target_type, target, file_format)
            return {'loaded': True}

        monkeypatch.setattr(dataops_mod, 'load', fake_load)

        assert (
            handlers.load_handler(
                source='-',
                target_type='api',
                target='endpoint',
                source_format=None,
                target_format=None,
                format_explicit=False,
                output=None,
                pretty=False,
            )
            == 0
        )
        assert read_calls['count'] == 1
        assert parse_calls['params'] == ('stdin-payload', None)
        assert load_record['params'] == (
            parsed_payload,
            'api',
            'endpoint',
            None,
        )
        kwargs = assert_emit_or_write(
            capture_io,
            {'loaded': True},
            None,
            pretty=False,
        )
        assert isinstance(kwargs['success_message'], str)

    def test_writes_output_file_and_skips_emit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`load_handler` writes to a file and skips STDOUT
        emission.
        """
        load_record: dict[str, object] = {}

        def fake_load(
            payload_obj: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (
                payload_obj,
                target_type,
                target,
                file_format,
            )
            return {'status': 'queued'}

        monkeypatch.setattr(dataops_mod, 'load', fake_load)

        assert (
            handlers.load_handler(
                source='payload.json',
                target_type='db',
                target='warehouse',
                source_format='json',
                target_format='json',
                format_explicit=True,
                output='result.json',
                pretty=True,
            )
            == 0
        )
        assert load_record['params'] == (
            'payload.json',
            'db',
            'warehouse',
            'json',
        )
        kwargs = assert_emit_or_write(
            capture_io,
            {'status': 'queued'},
            'result.json',
            pretty=True,
        )
        assert isinstance(kwargs['success_message'], str)


class TestSourceMappingPayloadHandlers:
    """Shared unit tests for handlers that require mapping side payloads."""

    @pytest.mark.parametrize(
        ('handler', 'mapping_name', 'mapping_arg', 'expected_error'),
        [
            pytest.param(
                handlers.transform_handler,
                'ops.json',
                'operations',
                'operations must resolve',
                id='transform',
            ),
            pytest.param(
                handlers.validate_handler,
                'rules.json',
                'rules',
                'rules must resolve',
                id='validate',
            ),
        ],
    )
    def test_requires_mapping_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        handler: Any,
        mapping_name: str,
        mapping_arg: str,
        expected_error: str,
    ) -> None:
        """
        Test that non-mapping side payloads raise :class:`ValueError`."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            {
                'data.json': [{'id': 1}],
                mapping_name: ['not-a-mapping'],
            },
        )

        with pytest.raises(ValueError, match=expected_error):
            handler(
                source='data.json',
                source_format='json',
                target=None,
                pretty=True,
                **{mapping_arg: mapping_name},
            )


class TestTransformHandler:
    """Unit tests for :func:`transform_handler`."""

    def test_emits_result_without_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`transform_handler` emits results with no target."""
        resolve_calls: list[ResolveCliPayloadCall] = []
        patch_resolve_cli_payload_map(
            monkeypatch,
            {'data.json': [{'id': 1}], 'ops.json': {'select': ['id']}},
            calls=resolve_calls,
        )
        monkeypatch.setattr(
            dataops_mod,
            'transform',
            lambda payload, ops: {'rows': payload, 'ops': ops},
        )

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                source_format='json',
                target=None,
                pretty=False,
            )
            == 0
        )
        assert resolve_calls == [
            ('data.json', 'json', True),
            ('ops.json', None, True),
        ]
        assert_emit_json(
            capture_io,
            {'rows': [{'id': 1}], 'ops': {'select': ['id']}},
            pretty=False,
        )

    def test_loads_non_file_target_via_connector(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that non-file targets delegate through :func:`load`."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            transform_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'transform',
            lambda payload, ops: {'payload': payload, 'ops': ops},
        )
        captured: dict[str, object] = {}

        def fake_load(
            source: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None = None,
        ) -> dict[str, object]:
            captured['params'] = (source, target_type, target, file_format)
            return {'status': 'success', 'target': target, 'target_type': target_type}

        monkeypatch.setattr(dataops_mod, 'load', fake_load)

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                target='https://example.com/items',
                target_type='api',
                pretty=False,
            )
            == 0
        )
        assert captured['params'] == (
            {
                'payload': {'source': 'data.json'},
                'ops': {'select': ['id']},
            },
            'api',
            'https://example.com/items',
            None,
        )
        assert_emit_json(
            capture_io,
            {
                'status': 'success',
                'target': 'https://example.com/items',
                'target_type': 'api',
            },
            pretty=False,
        )

    @pytest.mark.parametrize(
        'target',
        [
            pytest.param('s3://bucket/out.json', id='remote-uri'),
            pytest.param('out.json', id='local-file'),
        ],
    )
    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        target: str,
    ) -> None:
        """Test that :func:`transform_handler` writes data to file-like targets."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            transform_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'transform',
            lambda payload, ops: {'payload': payload, 'ops': ops},
        )
        write_calls = capture_file_write(monkeypatch)

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                target=target,
                target_format='json',
                pretty=True,
            )
            == 0
        )
        assert write_calls['params'] == (
            target,
            {
                'payload': {'source': 'data.json'},
                'ops': {'select': ['id']},
            },
        )
        assert f'Data transformed and saved to {target}' in capsys.readouterr().out


class TestValidateHandler:
    """Unit tests for :func:`validate_handler`."""

    @pytest.mark.parametrize(
        ('target', 'pretty', 'result', 'expected'),
        [
            pytest.param(
                None,
                False,
                {
                    'data': {'source': 'data.json'},
                    'rules': {'id': {'required': True}},
                },
                {
                    'data': {'source': 'data.json'},
                    'rules': {'id': {'required': True}},
                },
                id='no-target',
            ),
            pytest.param(
                '-',
                True,
                {
                    'data': {'source': 'data.json'},
                    'field_errors': {},
                    'rules': {'id': {'required': True}},
                    'valid': True,
                },
                {
                    'data': {'source': 'data.json'},
                    'field_errors': {},
                    'rules': {'id': {'required': True}},
                    'valid': True,
                },
                id='stdout-target',
            ),
        ],
    )
    def test_emits_json_when_not_writing_a_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
        target: str | None,
        pretty: bool,
        result: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that validation emits JSON unless it is writing a target file."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            validation_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'validate',
            lambda payload, rules: result,
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target=target,
                pretty=pretty,
            )
            == 0
        )
        assert_emit_json(capture_io, expected, pretty=pretty)

    def test_reports_missing_data_for_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`validate_handler` reports missing output data."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            validation_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'validate',
            lambda *_args, **_kwargs: {'data': None},
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target='out.json',
                pretty=True,
            )
            == 0
        )
        assert (
            'ValidationDict failed, no data to save for out.json'
            in capsys.readouterr().err
        )

    def test_rules_payload_resolves_even_when_format_is_explicit_elsewhere(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that rules files still resolve when other format state is explicit."""
        resolve_calls: list[ResolveCliPayloadCall] = []
        patch_resolve_cli_payload_map(
            monkeypatch,
            validation_payload_map(),
            calls=resolve_calls,
        )
        monkeypatch.setattr(
            dataops_mod,
            'validate',
            lambda payload, rules: {'data': payload, 'rules': rules},
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                source_format='json',
                format_explicit=True,
                pretty=False,
            )
            == 0
        )
        assert resolve_calls == [
            ('data.json', 'json', True),
            ('rules.json', None, True),
        ]
        assert_emit_json(
            capture_io,
            {
                'data': {'source': 'data.json'},
                'rules': {'id': {'required': True}},
            },
            pretty=False,
        )

    def test_schema_validation_reads_stdin_and_emits_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test schema validation reads STDIN sources and emits the full result."""
        calls: dict[str, object] = {}
        result = {'valid': True, 'errors': [], 'field_errors': {}, 'data': None}

        monkeypatch.setattr(handlers._input, 'read_stdin_text', lambda: '{"id": 1}')

        def fake_validate_schema(
            source: str,
            schema: str,
            *,
            schema_format: str | None,
            source_format: str | None,
        ) -> dict[str, object]:
            calls['params'] = (source, schema, schema_format, source_format)
            return result

        monkeypatch.setattr(dataops_mod, 'validate_schema', fake_validate_schema)

        assert (
            handlers.validate_handler(
                source='-',
                rules={},
                schema='schema.json',
                schema_format='jsonschema',
                source_format='json',
                pretty=False,
            )
            == 0
        )
        assert calls['params'] == ('{"id": 1}', 'schema.json', 'jsonschema', 'json')
        assert_emit_json(capture_io, result, pretty=False)

    def test_schema_validation_without_schema_format_emits_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test schema validation works when schema format is inferred."""
        result = {'valid': True, 'errors': [], 'field_errors': {}, 'data': None}

        monkeypatch.setattr(
            dataops_mod,
            'validate_schema',
            lambda *_args, **_kwargs: result,
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules={},
                schema='schema.json',
                pretty=False,
            )
            == 0
        )
        assert_emit_json(capture_io, result, pretty=False)

    def test_schema_validation_writes_result_to_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test schema validation writes the complete validation result."""
        result = {'valid': False, 'errors': ['bad'], 'field_errors': {}, 'data': None}
        write_calls: dict[str, object] = {}

        monkeypatch.setattr(
            dataops_mod,
            'validate_schema',
            lambda *_args, **_kwargs: result,
        )

        def fake_write(
            data: object,
            path: str | None,
            *,
            success_message: str,
        ) -> bool:
            write_calls['params'] = (data, path, success_message)
            return True

        monkeypatch.setattr(handlers._output, 'write_json_output', fake_write)

        assert (
            handlers.validate_handler(
                source='data.json',
                rules={},
                schema='schema.json',
                schema_format='jsonschema',
                target='result.json',
            )
            == 0
        )
        assert write_calls['params'] == (
            result,
            'result.json',
            'ValidationDict result saved to',
        )

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`validate_handler` writes data to a target file."""
        patch_resolve_cli_payload_map(
            monkeypatch,
            validation_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'validate',
            lambda *_args, **_kwargs: {'data': {'id': 1}},
        )
        write_calls: dict[str, object] = {}

        def fake_write(
            data: object,
            path: str | None,
            *,
            success_message: str,
        ) -> bool:
            write_calls['params'] = (data, path, success_message)
            return True

        monkeypatch.setattr(handlers._output, 'write_json_output', fake_write)

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target='out.json',
                pretty=True,
            )
            == 0
        )
        assert write_calls['params'] == (
            {'id': 1},
            'out.json',
            'ValidationDict result saved to',
        )
