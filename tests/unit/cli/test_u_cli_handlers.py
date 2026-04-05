"""
:mod:`tests.unit.cli.test_u_cli_handlers` module.

Unit tests for CLI handler implementation modules.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import ANY

import pytest

from etlplus import Config
from etlplus.cli._handlers import _completion as completion_mod
from etlplus.cli._handlers import _input as input_mod
from etlplus.cli._handlers import _lifecycle as lifecycle_mod
from etlplus.cli._handlers import _output as output_mod
from etlplus.cli._handlers import _summary as summary_mod
from etlplus.cli._handlers import check as check_mod
from etlplus.cli._handlers import dataops as dataops_mod
from etlplus.cli._handlers import init as init_mod
from etlplus.cli._handlers import render as render_mod
from etlplus.cli._handlers import run as run_mod
from etlplus.file import File
from etlplus.history import HistoryStore
from etlplus.history import RunCompletion
from etlplus.history import RunState
from etlplus.runtime import ReadinessReportBuilder
from etlplus.runtime import RuntimeEvents

from .conftest import CaptureIo
from .conftest import assert_emit_json
from .conftest import assert_emit_or_write

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


handlers: Any = SimpleNamespace(
    Config=Config,
    File=File,
    HistoryStore=HistoryStore,
    ReadinessReportBuilder=ReadinessReportBuilder,
    RunCompletion=RunCompletion,
    RunState=RunState,
    RuntimeEvents=RuntimeEvents,
    _CommandContext=lifecycle_mod.CommandContext,
    _check_sections=summary_mod.check_sections,
    _complete_output=completion_mod.complete_output,
    _failure_boundary=lifecycle_mod.failure_boundary,
    _input=input_mod,
    _output=output_mod,
    _pipeline_summary=summary_mod.pipeline_summary,
    _summary=summary_mod,
    check_handler=check_mod.check_handler,
    extract_handler=dataops_mod.extract_handler,
    init_handler=init_mod.init_handler,
    load_handler=dataops_mod.load_handler,
    render_handler=render_mod.render_handler,
    run_handler=run_mod.run_handler,
    transform_handler=dataops_mod.transform_handler,
    validate_handler=dataops_mod.validate_handler,
)

type _ResolveCliPayloadCall = tuple[object, str | None, bool]


def _capture_file_write(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, tuple[str, object]]:
    """Patch :meth:`File.write` and capture the written path plus payload."""
    captured: dict[str, tuple[str, object]] = {}

    def _write(self: File, data: object, **kwargs: object) -> None:
        captured['params'] = (str(self.path), data)

    monkeypatch.setattr(handlers.File, 'write', _write)
    return captured


def _patch_config_from_yaml(
    monkeypatch: pytest.MonkeyPatch,
    config: Config,
    *,
    calls: list[tuple[str, bool]] | None = None,
) -> None:
    """Patch :meth:`Config.from_yaml` to return one fixed config object."""

    def _from_yaml(path: str, substitute: bool) -> Config:
        if calls is not None:
            calls.append((path, substitute))
        return config

    monkeypatch.setattr(
        handlers.Config,
        'from_yaml',
        _from_yaml,
    )


def _patch_resolve_cli_payload_map(
    monkeypatch: pytest.MonkeyPatch,
    payloads: Mapping[object, object],
    *,
    calls: list[_ResolveCliPayloadCall] | None = None,
) -> None:
    """Patch :func:`resolve_cli_payload` with a fixed source-to-payload map."""

    def _resolve(
        source: object,
        *,
        format_hint: str | None,
        format_explicit: bool,
    ) -> object:
        if calls is not None:
            calls.append((source, format_hint, format_explicit))
        return payloads[source]

    monkeypatch.setattr(handlers._input, 'resolve_cli_payload', _resolve)


def _transform_payload_map() -> dict[object, object]:
    """Build the default payload map used by transform handler tests."""
    return {
        'data.json': {'source': 'data.json'},
        'ops.json': {'select': ['id']},
    }


def _validation_payload_map() -> dict[object, object]:
    """Build the default payload map used by validate handler tests."""
    return {
        'data.json': {'source': 'data.json'},
        'rules.json': {'id': {'required': True}},
    }


# SECTION: TESTS ============================================================ #


class TestCheckHandler:
    """Unit tests for :func:`check_handler`."""

    def test_graph_branch_emits_dag_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Graph mode should emit the ordered job graph summary."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'graph_summary',
            lambda _cfg: {
                'jobs': [{'depends_on': [], 'name': 'j1'}],
                'ordered_jobs': ['j1'],
                'status': 'ok',
            },
        )

        assert handlers.check_handler(config='cfg.yml', graph=True) == 0
        assert_emit_json(
            capture_io,
            {
                'jobs': [{'depends_on': [], 'name': 'j1'}],
                'ordered_jobs': ['j1'],
                'status': 'ok',
            },
            pretty=True,
        )

    def test_graph_branch_returns_error_payload_for_invalid_graph(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Graph mode should surface graph validation errors as JSON."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'graph_summary',
            lambda _cfg: (_ for _ in ()).throw(ValueError('Dependency cycle detected')),
        )

        assert handlers.check_handler(config='cfg.yml', graph=True) == 1
        assert_emit_json(
            capture_io,
            {
                'message': 'Dependency cycle detected',
                'status': 'error',
            },
            pretty=True,
        )

    def test_passes_substitute_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`check_handler` forwards the substitute flag to config
        loader.
        """
        load_calls: list[tuple[str, bool]] = []
        _patch_config_from_yaml(
            monkeypatch,
            dummy_cfg,
            calls=load_calls,
        )
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'pipelines': ['p1']},
        )
        assert handlers.check_handler(config='cfg.yml', substitute=True) == 0
        assert load_calls == [('cfg.yml', True)]
        assert_emit_json(capture_io, {'pipelines': ['p1']}, pretty=True)

    def test_prints_sections(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`check_handler` prints requested sections."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'targets': ['t1']},
        )
        assert handlers.check_handler(config='cfg.yml') == 0
        assert_emit_json(capture_io, {'targets': ['t1']}, pretty=True)

    def test_readiness_branch_emits_report_and_returns_nonzero_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that readiness mode emits the runtime report and exit code."""
        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'build',
            lambda config_path: {
                'checks': [],
                'status': 'error',
            },
        )

        assert handlers.check_handler(config='cfg.yml', readiness=True) == 1
        assert_emit_json(
            capture_io,
            {'checks': [], 'status': 'error'},
            pretty=True,
        )

    def test_requires_config_when_not_in_readiness_mode(self) -> None:
        """Non-readiness check mode should require a config path."""
        with pytest.raises(
            ValueError,
            match='config is required unless readiness-only mode is used',
        ):
            handlers.check_handler(config=None, readiness=False)

    def test_strict_branch_emits_report_and_skips_config_load_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that strict check mode should emit the strict report before config
        load.
        """
        config_loaded = {'value': False}

        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'strict_config_report',
            lambda config_path: {
                'checks': [{'name': 'config-structure', 'status': 'error'}],
                'status': 'error',
            },
        )
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda *_args, **_kwargs: config_loaded.__setitem__('value', True),
        )

        assert handlers.check_handler(config='cfg.yml', strict=True) == 1
        assert config_loaded['value'] is False
        assert_emit_json(
            capture_io,
            {
                'checks': [{'name': 'config-structure', 'status': 'error'}],
                'status': 'error',
            },
            pretty=True,
        )

    def test_strict_branch_falls_through_to_config_load_when_report_is_ok(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Strict check mode should continue into config loading on success."""
        monkeypatch.setattr(
            handlers.ReadinessReportBuilder,
            'strict_config_report',
            lambda config_path: {
                'checks': [{'name': 'config-structure', 'status': 'ok'}],
                'status': 'ok',
            },
        )
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'check_sections',
            lambda _cfg, **_kwargs: {'jobs': ['j1']},
        )

        assert handlers.check_handler(config='cfg.yml', strict=True) == 0
        assert_emit_json(capture_io, {'jobs': ['j1']}, pretty=True)

    def test_summary_branch_uses_pipeline_summary_with_requested_pretty_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that summary mode preserves the caller's pretty setting."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            check_mod._summary,
            'pipeline_summary',
            lambda _cfg: {'name': 'p1', 'jobs': ['j1']},
        )

        assert (
            handlers.check_handler(
                config='cfg.yml',
                summary=True,
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {'name': 'p1', 'jobs': ['j1']},
            pretty=False,
        )


class TestInitHandler:
    """Unit tests for :func:`init_handler`."""

    def test_overwrites_existing_scaffold_when_force_is_true(
        self,
        tmp_path: Path,
        capture_io: CaptureIo,
    ) -> None:
        """Init handler should overwrite scaffold files when force is enabled."""
        project_dir = tmp_path / 'starter'
        data_dir = project_dir / 'data'
        data_dir.mkdir(parents=True)
        (project_dir / 'pipeline.yml').write_text('name: old\n', encoding='utf-8')
        (data_dir / 'customers.csv').write_text('old,data\n', encoding='utf-8')

        assert handlers.init_handler(directory=str(project_dir), force=True) == 0
        assert 'Starter Pipeline' in (project_dir / 'pipeline.yml').read_text(
            encoding='utf-8',
        )
        assert 'Alice Example' in (data_dir / 'customers.csv').read_text(
            encoding='utf-8',
        )
        assert_emit_json(
            capture_io,
            cast(dict[str, Any], capture_io['emit_json'][0][0][0]),
            pretty=True,
        )

    def test_refuses_to_overwrite_existing_scaffold_without_force(
        self,
        tmp_path: Path,
    ) -> None:
        """Init handler should require force before overwriting scaffold files."""
        project_dir = tmp_path / 'starter'
        project_dir.mkdir()
        (project_dir / 'pipeline.yml').write_text('name: existing\n', encoding='utf-8')

        with pytest.raises(ValueError, match='Scaffold file already exists'):
            handlers.init_handler(directory=str(project_dir))

    def test_rejects_target_path_when_directory_argument_is_a_file(
        self,
        tmp_path: Path,
    ) -> None:
        """Init handler should reject a target path that already points to a file."""
        file_path = tmp_path / 'starter'
        file_path.write_text('not-a-directory\n', encoding='utf-8')

        with pytest.raises(ValueError, match='Init target must be a directory'):
            handlers.init_handler(directory=str(file_path))

    @pytest.mark.parametrize('conflicting_dir', ['data', 'temp'])
    def test_rejects_existing_file_where_scaffold_directory_is_required(
        self,
        tmp_path: Path,
        conflicting_dir: str,
    ) -> None:
        """Init handler should reject files where scaffold directories are needed."""
        project_dir = tmp_path / 'starter'
        project_dir.mkdir()
        (project_dir / conflicting_dir).write_text('conflict\n', encoding='utf-8')

        with pytest.raises(
            ValueError,
            match='Init target requires a directory but found a file',
        ):
            handlers.init_handler(directory=str(project_dir))

    def test_scaffolds_starter_files(
        self,
        tmp_path: Path,
        capture_io: CaptureIo,
    ) -> None:
        """Init handler should create starter files and emit a JSON payload."""
        project_dir = tmp_path / 'starter'

        assert handlers.init_handler(directory=str(project_dir)) == 0
        assert (project_dir / 'pipeline.yml').is_file()
        assert (project_dir / 'data' / 'customers.csv').is_file()
        payload = cast(dict[str, Any], capture_io['emit_json'][0][0][0])
        assert payload['status'] == 'ok'
        assert payload['job'] == 'file_to_file_customers'


class TestInitHandlerInternalHelpers:
    """Unit tests for internal helper functions in :mod:`etlplus.cli._handlers.init`."""

    def test_next_steps_omits_cd_when_root_matches_current_working_directory(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Next-step suggestions should omit cd when already in the target root."""
        monkeypatch.chdir(tmp_path)

        steps = init_mod._next_steps(tmp_path)

        assert steps == [
            'etlplus check --config pipeline.yml --jobs',
            'etlplus check --readiness --config pipeline.yml --strict',
            'etlplus run --config pipeline.yml --job file_to_file_customers',
        ]

    def test_write_text_file_rejects_directory_targets(
        self,
        tmp_path: Path,
    ) -> None:
        """Direct scaffold file writes should reject directory targets."""
        target_dir = tmp_path / 'pipeline.yml'
        target_dir.mkdir()

        with pytest.raises(
            ValueError,
            match='Cannot write scaffold file over directory',
        ):
            init_mod._write_text_file(
                target_dir,
                'payload\n',
                force=False,
            )


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`handlers`."""

    def test_check_sections_all(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` includes all requested sections."""
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_check_sections_default(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` defaults to jobs when no flags are
        set.
        """
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        assert 'jobs' in result

    def test_check_sections_jobs_and_mapping_transforms(
        self,
        dummy_cfg: Config,
    ) -> None:
        """Test that jobs flag plus mapping-style transforms extraction."""
        cfg = SimpleNamespace(
            name=dummy_cfg.name,
            version=dummy_cfg.version,
            sources=dummy_cfg.sources,
            targets=dummy_cfg.targets,
            jobs=dummy_cfg.jobs,
            transforms={
                'trim': {'field': 'name'},
                'dedupe': {'on': 'id'},
            },
        )

        result = handlers._check_sections(
            cast(Config, cfg),
            jobs=True,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=True,
        )
        assert result['jobs'] == ['j1']
        assert result['transforms'] == ['trim', 'dedupe']

    def test_complete_output_rejects_unknown_modes(self) -> None:
        """Unknown completion modes should fail fast with an assertion."""
        context = handlers._CommandContext(
            command='extract',
            event_format=None,
            run_id='run-123',
            started_at='2026-03-23T00:00:00Z',
            started_perf=0.0,
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(
                lifecycle_mod,
                'complete_command',
                lambda _context, **_fields: None,
            )
            with pytest.raises(AssertionError, match='Unsupported completion mode'):
                handlers._complete_output(
                    context,
                    {'ok': True},
                    mode=cast(Any, 'unsupported'),
                )

    def test_failure_boundary_invokes_on_error_callback(self) -> None:
        """The failure boundary should call its optional error callback."""
        captured: dict[str, object] = {}

        def on_error(exc: Exception) -> None:
            captured['exc'] = exc

        def fake_fail_command(
            context: object,
            exc: Exception,
            **fields: object,
        ) -> None:
            captured['context'] = context
            captured['failed'] = (exc, fields)

        context = handlers._CommandContext(
            command='extract',
            event_format=None,
            run_id='run-123',
            started_at='2026-03-23T00:00:00Z',
            started_perf=0.0,
        )

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(lifecycle_mod, 'fail_command', fake_fail_command)
            with pytest.raises(RuntimeError, match='boom'):
                with handlers._failure_boundary(
                    context,
                    on_error=on_error,
                    step='extract',
                ):
                    raise RuntimeError('boom')

        assert isinstance(captured['exc'], RuntimeError)
        failed_exc, failed_fields = cast(
            tuple[Exception, dict[str, object]],
            captured['failed'],
        )
        assert isinstance(failed_exc, RuntimeError)
        assert failed_fields == {'step': 'extract'}

    def test_pipeline_summary(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        summary = handlers._pipeline_summary(dummy_cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}

    def test_summary_collect_table_specs_merges_config_and_spec(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that config and standalone spec entries are merged."""
        spec_path = tmp_path / 'table.json'
        spec_path.write_text('{}', encoding='utf-8')
        monkeypatch.setattr(
            handlers._summary,
            'load_table_spec',
            lambda _path: {'table': 'from_spec'},
        )
        monkeypatch.setattr(
            handlers._summary.Config,
            'from_yaml',
            lambda _path, substitute: SimpleNamespace(
                table_schemas=[{'table': 'from_config'}],
            ),
        )

        specs = handlers._summary.collect_table_specs(
            config_path='pipeline.yml',
            spec_path=str(spec_path),
        )
        assert specs == [
            {'table': 'from_spec'},
            {'table': 'from_config'},
        ]


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
            lambda text, fmt: {'payload': text, 'fmt': fmt},
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
            {'payload': 'raw-text', 'fmt': None},
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
            handlers._input,
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

        parsed_payload = {'payload': 'stdin-payload', 'fmt': None}
        parse_calls: dict[str, object] = {}

        def fake_parse(text: str, fmt: str | None) -> object:
            parse_calls['params'] = (text, fmt)
            return parsed_payload

        monkeypatch.setattr(handlers._input, 'parse_text_payload', fake_parse)

        def fail_materialize(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                'materialize_file_payload should not be called for STDIN sources',
            )

        monkeypatch.setattr(
            handlers._input,
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


class TestRenderHandler:
    """Unit tests for :func:`render_handler`."""

    def test_errors_without_specs(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` reports missing specs."""

        assert (
            handlers.render_handler(
                config=None,
                spec=None,
                table=None,
                template='ddl',
                template_path=None,
                output=None,
                pretty=True,
                quiet=False,
            )
            == 1
        )
        assert 'No table schemas found' in capsys.readouterr().err

    def test_output_file_respects_quiet_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test output-file rendering without a status log in quiet mode."""
        output_path = tmp_path / 'rendered.sql'
        monkeypatch.setattr(
            handlers._summary,
            'collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        monkeypatch.setattr(
            render_mod,
            'render_tables',
            lambda specs, **kwargs: ['SELECT 1'],
        )

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table=None,
                template='ddl',
                template_path='custom.sql.j2',
                output=str(output_path),
                pretty=True,
                quiet=True,
            )
            == 0
        )
        assert output_path.read_text(encoding='utf-8') == 'SELECT 1\n'
        assert 'Rendered 1 schema(s)' not in capsys.readouterr().out

    def test_uses_template_file_override_when_template_is_a_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that template-path auto-detection from the ``template`` argument.
        """
        template_path = tmp_path / 'ddl.sql.j2'
        template_path.write_text('CREATE TABLE {{ table }}', encoding='utf-8')
        monkeypatch.setattr(
            handlers._summary,
            'collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        captured: dict[str, object] = {}

        def _render_tables(
            specs: list[dict[str, object]],
            *,
            template: str | None,
            template_path: str | None,
        ) -> list[str]:
            captured['specs'] = specs
            captured['template'] = template
            captured['template_path'] = template_path
            return ['SELECT 1']

        monkeypatch.setattr(render_mod, 'render_tables', _render_tables)

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table='Widget',
                template=cast(Any, str(template_path)),
                template_path=None,
                output='-',
                pretty=False,
                quiet=True,
            )
            == 0
        )
        assert captured['template'] is None
        assert captured['template_path'] == str(template_path)
        assert captured['specs'] == [{'table': 'Widget'}]
        assert capsys.readouterr().out.strip() == 'SELECT 1'

    def test_writes_sql_from_spec(
        self,
        widget_spec_paths: tuple[Path, Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` writes SQL for standalone specs."""
        spec_path, output_path = widget_spec_paths
        assert (
            handlers.render_handler(
                config=None,
                spec=str(spec_path),
                table=None,
                template='ddl',
                template_path=None,
                output=str(output_path),
                pretty=True,
                quiet=False,
            )
            == 0
        )

        sql_text = output_path.read_text(encoding='utf-8')
        assert 'CREATE TABLE [dbo].[Widget]' in sql_text

        captured = capsys.readouterr()
        assert f'Rendered 1 schema(s) to {output_path}' in captured.out


class TestRunHandler:
    """Unit tests for :func:`run_handler`."""

    def test_emits_pipeline_summary_without_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`run_handler` emits a summary when no job set."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job=None,
                pipeline=None,
                pretty=True,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            {
                'name': dummy_cfg.name,
                'version': dummy_cfg.version,
                'sources': ['s1'],
                'targets': ['t1'],
                'jobs': ['j1'],
            },
            pretty=True,
        )

    def test_runs_job_and_emits_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`run_handler` executes a named job and emits status.
        """
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-123',
        )

        history_calls: dict[str, object] = {}

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                history_calls['started'] = record

            def record_run_finished(
                self,
                completion: object,
            ) -> None:
                completion_obj = cast(handlers.RunCompletion, completion)
                history_calls['finished'] = {
                    'duration_ms': completion_obj.state.duration_ms,
                    'error_message': completion_obj.state.error_message,
                    'error_traceback': completion_obj.state.error_traceback,
                    'error_type': completion_obj.state.error_type,
                    'finished_at': completion_obj.state.finished_at,
                    'result_summary': completion_obj.state.result_summary,
                    'run_id': completion_obj.run_id,
                    'status': completion_obj.state.status,
                }

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            lambda: _FakeHistoryStore(),
        )
        run_calls: dict[str, object] = {}

        def fake_run(
            *,
            job: str,
            config_path: str,
            run_all: bool = False,
            continue_on_fail: bool = False,
        ) -> dict[str, object]:
            run_calls['params'] = (job, config_path, run_all, continue_on_fail)
            return {'job': job, 'ok': True}

        monkeypatch.setattr(run_mod, 'run', fake_run)

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                pretty=False,
            )
            == 0
        )
        assert run_calls['params'] == ('job1', 'pipeline.yml', False, False)
        assert cast(Any, history_calls['started']).run_id == 'run-123'
        assert history_calls['finished'] == {
            'duration_ms': ANY,
            'error_message': None,
            'error_traceback': None,
            'error_type': None,
            'finished_at': ANY,
            'result_summary': {'job': 'job1', 'ok': True},
            'run_id': 'run-123',
            'status': 'succeeded',
        }
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-123',
                'status': 'ok',
                'result': {'job': 'job1', 'ok': True},
            },
            pretty=False,
        )

    def test_run_all_failure_summary_returns_nonzero_and_records_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Handled DAG failures should emit JSON, record failure, and return 1."""
        _patch_config_from_yaml(monkeypatch, dummy_cfg)
        monkeypatch.setattr(
            handlers.RuntimeEvents,
            'create_run_id',
            lambda: 'run-all-1',
        )

        history_calls: dict[str, object] = {}

        class _FakeHistoryStore:
            def record_run_started(self, record: object) -> None:
                history_calls['started'] = record

            def record_run_finished(
                self,
                completion: object,
            ) -> None:
                completion_obj = cast(handlers.RunCompletion, completion)
                history_calls['finished'] = completion_obj.state

        monkeypatch.setattr(
            handlers.HistoryStore,
            'from_environment',
            lambda: _FakeHistoryStore(),
        )
        lifecycle_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            run_mod._lifecycle,
            'emit_lifecycle_event',
            lambda **kwargs: lifecycle_calls.append(kwargs),
        )
        monkeypatch.setattr(
            run_mod,
            'run',
            lambda **kwargs: {
                'continue_on_fail': True,
                'executed_jobs': [
                    {'job': 'seed', 'status': 'failed'},
                    {
                        'job': 'publish',
                        'reason': 'upstream_failed',
                        'skipped_due_to': ['seed'],
                        'status': 'skipped',
                    },
                ],
                'failed_jobs': ['seed'],
                'mode': 'all',
                'ordered_jobs': ['seed', 'publish'],
                'skipped_jobs': ['publish'],
                'status': 'failed',
                'succeeded_jobs': [],
            },
        )

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                run_all=True,
                continue_on_fail=True,
                pretty=False,
            )
            == 1
        )
        assert cast(Any, history_calls['started']).job_name is None
        finished = cast(handlers.RunState, history_calls['finished'])
        assert finished.status == 'failed'
        assert finished.error_type == 'RunExecutionFailed'
        assert 'DAG execution' in cast(str, finished.error_message)
        assert [call['lifecycle'] for call in lifecycle_calls] == [
            'started',
            'failed',
        ]
        assert_emit_json(
            capture_io,
            {
                'run_id': 'run-all-1',
                'status': 'error',
                'result': {
                    'continue_on_fail': True,
                    'executed_jobs': [
                        {'job': 'seed', 'status': 'failed'},
                        {
                            'job': 'publish',
                            'reason': 'upstream_failed',
                            'skipped_due_to': ['seed'],
                            'status': 'skipped',
                        },
                    ],
                    'failed_jobs': ['seed'],
                    'mode': 'all',
                    'ordered_jobs': ['seed', 'publish'],
                    'skipped_jobs': ['publish'],
                    'status': 'failed',
                    'succeeded_jobs': [],
                },
            },
            pretty=False,
        )


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
        """Non-mapping side payloads should raise :class:`ValueError`."""
        _patch_resolve_cli_payload_map(
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
        resolve_calls: list[tuple[object, str | None, bool]] = []
        _patch_resolve_cli_payload_map(
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
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _transform_payload_map(),
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
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _transform_payload_map(),
        )
        monkeypatch.setattr(
            dataops_mod,
            'transform',
            lambda payload, ops: {'payload': payload, 'ops': ops},
        )
        write_calls = _capture_file_write(monkeypatch)

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
        """Validation should emit JSON unless it is writing a target file."""
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _validation_payload_map(),
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
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _validation_payload_map(),
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
        resolve_calls: list[tuple[object, str | None, bool]] = []
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _validation_payload_map(),
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

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`validate_handler` writes data to a target file."""
        _patch_resolve_cli_payload_map(
            monkeypatch,
            _validation_payload_map(),
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
