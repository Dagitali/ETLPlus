"""
:mod:`tests.unit.ops.test_u_ops_run` module.

Unit tests for :mod:`etlplus.ops.run`.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import ClassVar
from typing import Self
from typing import cast

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


run_mod = importlib.import_module('etlplus.ops.run')
extract_mod = importlib.import_module('etlplus.ops.extract')
load_mod = importlib.import_module('etlplus.ops.load')


def _base_config(
    job: SimpleNamespace,
    source: SimpleNamespace,
    target: SimpleNamespace,
) -> SimpleNamespace:
    return SimpleNamespace(
        jobs=[job],
        sources=[source],
        targets=[target],
        transforms={'noop': {}},
        validations={},
    )


def _make_job(
    *,
    name: str,
    source: str,
    target: str,
    depends_on: list[str] | None = None,
    options: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        depends_on=[] if depends_on is None else list(depends_on),
        name=name,
        extract=SimpleNamespace(source=source, options=options or {}),
        transform=SimpleNamespace(pipeline='noop'),
        load=SimpleNamespace(target=target, overrides=None),
        validate=None,
    )


# SECTION: TESTS ============================================================ #


class TestRun:
    """Unit tests for :func:`etlplus.ops.run.run`."""

    def test_api_source_and_target_pipeline(
        self,
        base_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test an API-to-API ETL pipeline execution."""
        job = _make_job(name='api_job', source='api_src', target='api_tgt')
        cfg = _base_config(
            job,
            SimpleNamespace(name='api_src', type='api'),
            SimpleNamespace(name='api_tgt', type='api'),
        )

        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        req_env = {
            'use_endpoints': True,
            'base_url': base_url,
            'base_path': '/v1',
            'endpoints_map': {'users': '/users'},
            'endpoint_key': 'users',
            'params': {'limit': 5},
            'headers': {'Accept': 'json'},
            'timeout': 5,
            'pagination': {'type': 'page'},
            'sleep_seconds': 0.2,
            'retry': {'max_attempts': 2},
            'retry_network_errors': True,
            'session': 'session-token',
        }
        monkeypatch.setattr(
            extract_mod,
            'compose_api_request_env',
            lambda cfg_obj, source_obj, opts: req_env,
        )

        class DummyClient:
            """Dummy EndpointClient for testing purposes."""

            instances: ClassVar[list[Self]] = []

            def __init__(self, **kwargs: Any) -> None:
                self.kwargs = kwargs
                DummyClient.instances.append(self)

        monkeypatch.setattr(extract_mod, 'EndpointClient', DummyClient)

        paginate_calls: list[dict[str, Any]] = []

        def _capture_paginate(
            client: Any,
            endpoint_key: str,
            params: Any,
            headers: Any,
            timeout: Any,
            pagination: Any,
            sleep_seconds: Any,
        ) -> list[dict[str, int]]:
            paginate_calls.append(
                {
                    'client': client,
                    'endpoint_key': endpoint_key,
                    'params': params,
                    'headers': headers,
                    'timeout': timeout,
                    'pagination': pagination,
                    'sleep_seconds': sleep_seconds,
                },
            )
            return [{'id': 1}]

        monkeypatch.setattr(
            extract_mod,
            'paginate_with_client',
            _capture_paginate,
        )

        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, stage, **kwargs: data,
        )

        monkeypatch.setattr(run_mod, 'transform', lambda data, ops: data)

        target_env = {
            'url': 'https://sink.example.com',
            'method': 'put',
            'headers': {'Auth': 'token'},
            'timeout': 7,
            'session': 'target-session',
        }
        monkeypatch.setattr(
            load_mod,
            'compose_api_target_env',
            lambda cfg_obj, target_obj, overrides: target_env,
        )

        load_calls: list[tuple] = []

        def _capture_load_env(
            data: Any,
            env: dict[str, Any],
        ) -> dict[str, bool]:
            load_calls.append((data, env))
            return {'ok': True}

        monkeypatch.setattr(load_mod, '_load_to_api_env', _capture_load_env)

        result = run_mod.run('api_job')

        assert DummyClient.instances
        assert paginate_calls[0]['endpoint_key'] == 'users'
        assert paginate_calls[0]['params'] == {'limit': 5}
        assert load_calls[0][1]['url'] == 'https://sink.example.com'
        assert load_calls[0][1]['method'] == 'put'
        assert result == {'ok': True}

    def test_database_source_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`run` extracts from database connectors."""
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(
            name='src',
            type='database',
            connection_string='sqlite:///source.db',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path='/tmp/out.json',
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        extract_calls: list[tuple[str, str]] = []

        def _extract(
            stype: str,
            source: str,
            **kwargs: Any,
        ) -> list[dict[str, int]]:
            extract_calls.append((stype, source))
            return [{'id': 1}]

        monkeypatch.setattr(
            run_mod,
            'extract',
            _extract,
        )
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(
            run_mod,
            'load',
            lambda *_a, **_k: {'status': 'ok'},
        )

        result = run_mod.run('job')

        assert extract_calls == [('database', 'sqlite:///source.db')]
        assert result == {'status': 'ok'}

    def test_database_target_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`run` loads to database connectors."""
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(
            name='src',
            type='file',
            path='/tmp/in.json',
            format='json',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='database',
            connection_string='sqlite:///target.db',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: [{'id': 1}])
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        load_calls: list[tuple[str, str]] = []

        def _load(
            data: Any,
            target_type: str,
            target: str,
            **kwargs: Any,
        ) -> dict[str, str]:
            load_calls.append((target_type, target))
            return {'status': 'ok'}

        monkeypatch.setattr(
            run_mod,
            'load',
            _load,
        )

        result = run_mod.run('job')

        assert load_calls == [('database', 'sqlite:///target.db')]
        assert result == {'status': 'ok'}

    def test_file_pipeline_merges_options_and_preserves_remote_uris(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test file connector option forwarding for remote pipeline paths."""
        job = _make_job(
            name='file_job',
            source='file_src',
            target='file_tgt',
            options={'delimiter': '|', 'sheet': 'Sheet1'},
        )
        job.load = SimpleNamespace(
            target='file_tgt',
            overrides={'encoding': 'utf-16', 'delimiter': ';'},
        )
        cfg = _base_config(
            job,
            SimpleNamespace(
                name='file_src',
                type='file',
                path='s3://bucket/input.csv',
                format='csv',
                options={'encoding': 'latin-1', 'delimiter': ','},
            ),
            SimpleNamespace(
                name='file_tgt',
                type='file',
                path='s3://bucket/output.csv',
                format='csv',
                options={'encoding': 'utf-8'},
            ),
        )

        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        extract_calls: list[tuple[Any, Any, dict[str, Any]]] = []

        def _capture_extract(
            stype: str,
            source: str,
            **kwargs: Any,
        ) -> list[dict[str, int]]:
            extract_calls.append((stype, source, kwargs))
            return [{'id': 1}]

        monkeypatch.setattr(run_mod, 'extract', _capture_extract)
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)

        load_calls: list[tuple[Any, Any, Any, dict[str, Any]]] = []

        def _capture_load(
            data: Any,
            connector: str,
            target: str,
            **kwargs: Any,
        ) -> dict[str, str]:
            load_calls.append((data, connector, target, kwargs))
            return {'status': 'ok'}

        monkeypatch.setattr(run_mod, 'load', _capture_load)

        result = run_mod.run('file_job')

        assert result == {'status': 'ok'}
        assert extract_calls == [
            (
                'file',
                's3://bucket/input.csv',
                {
                    'file_format': 'csv',
                    'encoding': 'latin-1',
                    'delimiter': '|',
                    'sheet': 'Sheet1',
                },
            ),
        ]
        assert load_calls == [
            (
                [{'id': 1}],
                'file',
                's3://bucket/output.csv',
                {
                    'file_format': 'csv',
                    'encoding': 'utf-16',
                    'delimiter': ';',
                },
            ),
        ]

    def test_file_source_missing_path_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that file source missing path raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(name='src', type='file', format='json')
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path='/tmp/out.json',
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match='File source missing "path"'):
            run_mod.run('job')

    def test_file_target_missing_path_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that file target missing path raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        src_path = tmp_path / 'in.json'
        src_path.write_text('[]', encoding='utf-8')
        src = SimpleNamespace(
            name='src',
            type='file',
            path=str(src_path),
            format='json',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(
            ValueError,
            match=r'(?i)(file target).*path|missing\s+"?path"?',
        ):
            run_mod.run('job')

    def test_file_to_file_pipeline(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that a file-to-file ETL pipeline execution."""
        job = _make_job(name='file_job', source='file_src', target='file_tgt')
        cfg = _base_config(
            job,
            SimpleNamespace(
                name='file_src',
                type='file',
                path='/tmp/input.json',
                format='json',
            ),
            SimpleNamespace(
                name='file_tgt',
                type='file',
                path='/tmp/output.json',
                format='json',
            ),
        )

        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        extract_calls: list[tuple] = []

        def _capture_extract(
            stype: str,
            source: str,
            **kwargs: Any,
        ) -> list[dict[str, int]]:
            extract_calls.append((stype, source, kwargs))
            return [{'id': 1}]

        monkeypatch.setattr(run_mod, 'extract', _capture_extract)

        transform_calls: list[Any] = []

        def _capture_transform(data: Any, ops: Any) -> dict[str, Any]:
            transform_calls.append((data, ops))
            return {'payload': data}

        monkeypatch.setattr(run_mod, 'transform', _capture_transform)

        stages: list[str] = []

        def _capture_validate(data: Any, stage: str, **kwargs: Any) -> Any:
            stages.append(stage)
            return data

        monkeypatch.setattr(run_mod, 'maybe_validate', _capture_validate)

        load_calls: list[tuple] = []

        def _capture_load_file(
            data: Any,
            connector: str,
            target: str,
            **kwargs: Any,
        ) -> dict[str, str]:
            load_calls.append((data, connector, target, kwargs))
            return {'status': 'ok'}

        monkeypatch.setattr(run_mod, 'load', _capture_load_file)

        result = run_mod.run('file_job')

        assert extract_calls[0][0] == 'file'
        assert extract_calls[0][1] == '/tmp/input.json'
        assert transform_calls
        assert stages == ['before_transform', 'after_transform']
        assert load_calls[0][1] == 'file'
        assert load_calls[0][2] == '/tmp/output.json'
        assert result == {'status': 'ok'}

    def test_run_all_executes_jobs_in_topological_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``run_all`` should execute every configured job in DAG order."""
        seed_job = _make_job(
            name='seed',
            source='seed_src',
            target='seed_tgt',
        )
        clean_job = _make_job(
            name='clean',
            source='clean_src',
            target='clean_tgt',
            depends_on=['seed'],
        )
        publish_job = _make_job(
            name='publish',
            source='publish_src',
            target='publish_tgt',
            depends_on=['clean'],
        )
        cfg = SimpleNamespace(
            jobs=[publish_job, clean_job, seed_job],
            sources=[
                SimpleNamespace(
                    name='seed_src',
                    type='file',
                    path='/tmp/seed.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='clean_src',
                    type='file',
                    path='/tmp/clean.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='publish_src',
                    type='file',
                    path='/tmp/publish.json',
                    format='json',
                ),
            ],
            targets=[
                SimpleNamespace(
                    name='seed_tgt',
                    type='file',
                    path='/tmp/seed-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='clean_tgt',
                    type='file',
                    path='/tmp/clean-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='publish_tgt',
                    type='file',
                    path='/tmp/publish-out.json',
                    format='json',
                ),
            ],
            transforms={'noop': {}},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(
            run_mod,
            'extract',
            lambda _stype, source, **_kwargs: [{'source': source}],
        )
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_args, **_kwargs: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(
            run_mod,
            'load',
            lambda *_args, **kwargs: {
                'status': 'success',
                'target': kwargs.get('file_format'),
            },
        )

        result = run_mod.run(run_all=True)

        assert result['status'] == 'success'
        assert result['mode'] == 'all'
        assert result['ordered_jobs'] == ['seed', 'clean', 'publish']
        assert [job['job'] for job in result['executed_jobs']] == [
            'seed',
            'clean',
            'publish',
        ]

    def test_run_all_continue_on_fail_skips_blocked_downstream_jobs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Continue-on-fail should run independent jobs and skip blocked ones."""
        seed_job = _make_job(
            name='alpha_seed',
            source='seed_src',
            target='seed_tgt',
        )
        report_job = _make_job(
            name='beta_report',
            source='report_src',
            target='report_tgt',
        )
        main_job = _make_job(
            name='gamma_main',
            source='main_src',
            target='main_tgt',
            depends_on=['alpha_seed'],
        )
        cfg = SimpleNamespace(
            jobs=[main_job, report_job, seed_job],
            sources=[
                SimpleNamespace(
                    name='seed_src',
                    type='file',
                    path='/tmp/seed.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='report_src',
                    type='file',
                    path='/tmp/report.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_src',
                    type='file',
                    path='/tmp/main.json',
                    format='json',
                ),
            ],
            targets=[
                SimpleNamespace(
                    name='seed_tgt',
                    type='file',
                    path='/tmp/seed-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='report_tgt',
                    type='file',
                    path='/tmp/report-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_tgt',
                    type='file',
                    path='/tmp/main-out.json',
                    format='json',
                ),
            ],
            transforms={'noop': {}},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        def _extract(
            _stype: str,
            source: str,
            **kwargs: Any,
        ) -> list[dict[str, str]]:
            del kwargs
            if source == '/tmp/seed.json':
                raise ValueError('seed boom')
            return [{'source': source}]

        monkeypatch.setattr(run_mod, 'extract', _extract)
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_args, **_kwargs: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(
            run_mod,
            'load',
            lambda *_args, **_kwargs: {'status': 'success'},
        )

        result = run_mod.run(
            run_all=True,
            continue_on_fail=True,
        )

        assert result['status'] == 'partial_success'
        assert result['ordered_jobs'] == ['alpha_seed', 'beta_report', 'gamma_main']
        assert result['failed_jobs'] == ['alpha_seed']
        assert result['skipped_jobs'] == ['gamma_main']
        assert result['succeeded_jobs'] == ['beta_report']
        assert result['failed_job_count'] == 1
        assert result['skipped_job_count'] == 1
        assert result['succeeded_job_count'] == 1
        assert result['executed_job_count'] == 2
        assert result['executed_jobs'][0]['status'] == 'failed'
        assert result['executed_jobs'][1]['status'] == 'succeeded'
        assert result['executed_jobs'][2] == {
            'job': 'gamma_main',
            'reason': 'upstream_failed',
            'skipped_due_to': ['alpha_seed'],
            'status': 'skipped',
        }

    def test_run_executes_dependency_closure_in_dag_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Dependency-aware runs should execute prerequisites before the job."""
        seed_job = _make_job(
            name='seed',
            source='seed_src',
            target='seed_tgt',
        )
        main_job = _make_job(
            name='main',
            source='main_src',
            target='main_tgt',
            depends_on=['seed'],
        )
        cfg = SimpleNamespace(
            jobs=[main_job, seed_job],
            sources=[
                SimpleNamespace(
                    name='seed_src',
                    type='file',
                    path='/tmp/seed.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_src',
                    type='file',
                    path='/tmp/main.json',
                    format='json',
                ),
            ],
            targets=[
                SimpleNamespace(
                    name='seed_tgt',
                    type='file',
                    path='/tmp/seed-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_tgt',
                    type='file',
                    path='/tmp/main-out.json',
                    format='json',
                ),
            ],
            transforms={'noop': {}},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(
            run_mod,
            'extract',
            lambda _stype, source, **_kwargs: [{'source': source}],
        )
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_args, **_kwargs: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)

        load_calls: list[str] = []

        def _capture_load(
            data: Any,
            connector: str,
            target: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            del data, connector, kwargs
            load_calls.append(target)
            return {'status': 'success', 'target': target}

        monkeypatch.setattr(run_mod, 'load', _capture_load)

        result = run_mod.run('main')

        assert result['status'] == 'success'
        assert result['mode'] == 'job'
        assert result['requested_job'] == 'main'
        assert result['ordered_jobs'] == ['seed', 'main']
        assert [job['job'] for job in result['executed_jobs']] == ['seed', 'main']
        assert result['succeeded_jobs'] == ['seed', 'main']
        assert result['failed_jobs'] == []
        assert result['skipped_jobs'] == []
        assert result['final_job'] == 'main'
        assert result['final_result'] == {
            'status': 'success',
            'target': '/tmp/main-out.json',
        }
        assert load_calls == ['/tmp/seed-out.json', '/tmp/main-out.json']

    def test_run_accepts_tuple_dependencies_in_dag_planning(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tuple-based dependencies should be treated like configured lists."""
        seed_job = _make_job(
            name='seed',
            source='seed_src',
            target='seed_tgt',
        )
        main_job = _make_job(
            name='main',
            source='main_src',
            target='main_tgt',
        )
        main_job.depends_on = ('seed',)
        cfg = SimpleNamespace(
            jobs=[main_job, seed_job],
            sources=[
                SimpleNamespace(
                    name='seed_src',
                    type='file',
                    path='/tmp/seed.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_src',
                    type='file',
                    path='/tmp/main.json',
                    format='json',
                ),
            ],
            targets=[
                SimpleNamespace(
                    name='seed_tgt',
                    type='file',
                    path='/tmp/seed-out.json',
                    format='json',
                ),
                SimpleNamespace(
                    name='main_tgt',
                    type='file',
                    path='/tmp/main-out.json',
                    format='json',
                ),
            ],
            transforms={'noop': {}},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(
            run_mod,
            'extract',
            lambda _stype, source, **_kwargs: [{'source': source}],
        )
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_args, **_kwargs: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(
            run_mod,
            'load',
            lambda *_args, **_kwargs: {'status': 'success'},
        )

        result = run_mod.run('main')

        assert result['ordered_jobs'] == ['seed', 'main']

    def test_run_raises_when_load_result_is_not_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Run should reject non-mapping terminal load results."""
        job = _make_job(name='job', source='src', target='tgt')
        cfg = _base_config(
            job,
            SimpleNamespace(
                name='src',
                type='file',
                path='/tmp/in.json',
                format='json',
            ),
            SimpleNamespace(
                name='tgt',
                type='file',
                path='/tmp/out.json',
                format='json',
            ),
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: {'id': 1})
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(run_mod, 'load', lambda *_a, **_k: ['bad-result'])

        with pytest.raises(TypeError, match='load result must be a mapping'):
            run_mod.run('job')

    @pytest.mark.parametrize(
        ('cfg', 'expected_message'),
        [
            (
                SimpleNamespace(
                    jobs=[],
                    sources=[],
                    targets=[],
                    transforms={},
                    validations={},
                ),
                'No jobs configured',
            ),
            (
                _base_config(
                    _make_job(name='other', source='src', target='tgt'),
                    SimpleNamespace(
                        name='src',
                        type='file',
                        path='/tmp/in.json',
                        format='json',
                    ),
                    SimpleNamespace(
                        name='tgt',
                        type='file',
                        path='/tmp/out.json',
                        format='json',
                    ),
                ),
                'Job not found',
            ),
        ],
        ids=['no-jobs', 'different-job'],
    )
    def test_job_not_found_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cfg: Any,
        expected_message: str,
    ) -> None:
        """Test that requesting a missing job raises :class:`ValueError`."""
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        with pytest.raises(ValueError, match=expected_message):
            run_mod.run('missing')

    def test_load_missing_section_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that missing load section raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        job.load = None
        src_path = tmp_path / 'in.json'
        src_path.write_text('[]', encoding='utf-8')
        tgt_path = tmp_path / 'out.json'
        tgt_path.write_text('[]', encoding='utf-8')

        src = SimpleNamespace(
            name='src',
            type='file',
            path=str(src_path),
            format='json',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path=str(tgt_path),
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match=r'(?i)load'):
            run_mod.run('job')

    def test_missing_extract_section_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that missing extract section raises :class:`ValueError`."""
        job = SimpleNamespace(
            name='job',
            extract=None,
            transform=None,
            load=None,
            validate=None,
        )
        cfg = SimpleNamespace(
            jobs=[job],
            sources=[],
            targets=[],
            transforms={},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match='extract'):
            run_mod.run('job')

    def test_run_defensive_source_dispatch_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unexpected source coercion triggers :class:`ValueError`.
        """
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(name='src', type='file', path='/tmp/in.json')
        tgt = SimpleNamespace(name='tgt', type='file', path='/tmp/out.json')
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(
            run_mod.DataConnectorType,
            'coerce',
            classmethod(lambda cls, value: object()),
        )

        with pytest.raises(ValueError, match='Unsupported source type'):
            run_mod.run('job')

    def test_run_defensive_target_dispatch_branch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unexpected target coercion triggers :class:`ValueError`.
        """
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(name='src', type='file', path='/tmp/in.json')
        tgt = SimpleNamespace(name='tgt', type='file', path='/tmp/out.json')
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: [{'id': 1}])
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        call_count = {'value': 0}

        def _coerce(cls, value):  # noqa: ANN001, ANN202
            call_count['value'] += 1
            if call_count['value'] == 1:
                return run_mod.DataConnectorType.FILE
            return object()

        monkeypatch.setattr(
            run_mod.DataConnectorType,
            'coerce',
            classmethod(_coerce),
        )

        with pytest.raises(ValueError, match='Unsupported target type'):
            run_mod.run('job')

    def test_run_skips_transform_when_not_configured(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that jobs without transform config bypass transform step.
        """
        job = _make_job(name='job', source='src', target='tgt')
        job.transform = None
        src = SimpleNamespace(
            name='src',
            type='file',
            path='/tmp/in.json',
            format='json',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path='/tmp/out.json',
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: [{'id': 1}])

        stages: list[str] = []

        def _capture_validate(data: Any, stage: str, **kwargs: Any) -> Any:
            stages.append(stage)
            return data

        monkeypatch.setattr(run_mod, 'maybe_validate', _capture_validate)

        transform_calls: list[tuple[Any, Any]] = []

        def _capture_transform(data: Any, ops: Any) -> Any:
            transform_calls.append((data, ops))
            return data

        monkeypatch.setattr(run_mod, 'transform', _capture_transform)
        monkeypatch.setattr(run_mod, 'load', lambda *_a, **_k: {'ok': True})

        result = run_mod.run('job')

        assert not transform_calls
        assert stages == ['before_transform', 'after_transform']
        assert result == {'ok': True}

    def test_transform_and_validation_branches(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that transform and validation branches are called."""
        job = _make_job(name='job', source='src', target='tgt')
        job.transform = SimpleNamespace(pipeline='noop')
        job.validate = SimpleNamespace(
            ruleset='rules',
            phase='phase',
            severity='severity',
        )
        src_path = tmp_path / 'in.json'
        src_path.write_text('[]', encoding='utf-8')
        tgt_path = tmp_path / 'out.json'
        tgt_path.write_text('[]', encoding='utf-8')

        src = SimpleNamespace(
            name='src',
            type='file',
            path=str(src_path),
            format='json',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path=str(tgt_path),
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        cfg.validations = {'rules': {}}

        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )

        monkeypatch.setattr(run_mod, 'extract', lambda *a, **k: [{'id': 1}])

        validate_stages: list[str] = []

        def _capture_validate(data: Any, stage: str, **kwargs: Any) -> Any:
            validate_stages.append(stage)
            return data

        monkeypatch.setattr(run_mod, 'maybe_validate', _capture_validate)

        transform_calls: list[tuple[Any, Any]] = []

        def _capture_transform(data, ops):
            transform_calls.append((data, ops))
            return data

        monkeypatch.setattr(run_mod, 'transform', _capture_transform)

        load_calls: list[tuple[Any, str, str]] = []

        def _capture_load(data, connector, path, **kwargs):
            load_calls.append((data, connector, path))
            return {'status': 'ok'}

        monkeypatch.setattr(run_mod, 'load', _capture_load)

        result = run_mod.run('job')

        assert validate_stages[:1] == ['before_transform']
        assert validate_stages[-1:] == ['after_transform']
        assert transform_calls
        assert load_calls == [([{'id': 1}], 'file', str(tgt_path))]
        assert result == {'status': 'ok'}

    def test_unknown_source_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unknown source raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        cfg = SimpleNamespace(
            jobs=[job],
            sources=[],
            targets=[],
            transforms={},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match='Unknown source'):
            run_mod.run('job')

    def test_unknown_target_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unknown target raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        src_path = tmp_path / 'in.json'
        src_path.write_text('[]', encoding='utf-8')
        src = SimpleNamespace(
            name='src',
            type='file',
            path=str(src_path),
            format='json',
        )
        cfg = SimpleNamespace(
            jobs=[job],
            sources=[src],
            targets=[],
            transforms={},
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match=r'(?i)target'):
            run_mod.run('job')

    def test_unsupported_source_type_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unsupported source type raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        src = SimpleNamespace(
            name='src',
            type='unsupported',
        )
        tgt = SimpleNamespace(
            name='tgt',
            type='file',
            path='/tmp/out.json',
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match=r'(?i)unsupported'):
            run_mod.run('job')

    def test_unsupported_target_type_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unsupported target type raises :class:`ValueError`."""
        job = _make_job(name='job', source='src', target='tgt')
        src_path = tmp_path / 'in.json'
        src_path.write_text('[]', encoding='utf-8')
        src = SimpleNamespace(
            name='src',
            type='file',
            path=str(src_path),
            format='json',
        )
        tgt_path = tmp_path / 'out.json'
        tgt_path.write_text('[]', encoding='utf-8')
        tgt = SimpleNamespace(
            name='tgt',
            type='unsupported',
            path=str(tgt_path),
            format='json',
        )
        cfg = _base_config(job, src, tgt)
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        with pytest.raises(ValueError, match=r'(?i)unsupported'):
            run_mod.run('job')


class TestRunInternals:
    """Unit tests for internal run() helpers."""

    def test_delegates_to_load_when_target_is_provided(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that run_pipeline forwards to load() for terminal write steps.
        """
        load_calls: list[tuple[Any, Any, Any]] = []

        def _load(
            data: Any,
            target_type: Any,
            target: Any,
            **kwargs: Any,
        ) -> dict[str, str]:
            load_calls.append((data, target_type, target))
            return {'status': 'ok'}

        monkeypatch.setattr(
            run_mod,
            'load',
            _load,
        )

        result = run_mod.run_pipeline(
            source_type=None,
            source={'id': 1},
            target_type='file',
            target='/tmp/out.json',
        )

        assert result == {'status': 'ok'}
        assert load_calls == [({'id': 1}, 'file', '/tmp/out.json')]

    def test_dispatch_extract_uses_api_source_connector_loader(self) -> None:
        """API source dispatch should prefer connector-aware extraction."""
        calls: list[tuple[Any, Any, dict[str, Any]]] = []

        def _extract_from_api_source(
            cfg: Any,
            connector_obj: Any,
            overrides: dict[str, Any],
        ) -> dict[str, bool]:
            calls.append((cfg, connector_obj, overrides))
            return {'ok': True}

        cfg = SimpleNamespace(name='cfg')
        connector = SimpleNamespace(name='api_src')
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            run_mod,
            'extract_from_api_source',
            _extract_from_api_source,
        )
        try:
            result = run_mod._dispatch_extract(
                'api',
                'https://example.test/items',
                options={'timeout': 2.0},
                cfg=cfg,
                connector_obj=connector,
            )
        finally:
            monkeypatch.undo()

        assert result == {'ok': True}
        assert calls == [(cfg, connector, {'timeout': 2.0})]

    def test_dispatch_load_uses_api_target_connector_loader(self) -> None:
        """API target dispatch should prefer connector-aware loading."""
        calls: list[tuple[Any, Any, dict[str, Any], Any]] = []

        def _load_to_api_target(
            cfg: Any,
            connector_obj: Any,
            overrides: dict[str, Any],
            data: Any,
        ) -> dict[str, bool]:
            calls.append((cfg, connector_obj, overrides, data))
            return {'ok': True}

        cfg = SimpleNamespace(name='cfg')
        connector = SimpleNamespace(name='api_tgt')
        payload = {'id': 1}
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            run_mod,
            'load_to_api_target',
            _load_to_api_target,
        )
        try:
            result = run_mod._dispatch_load(
                payload,
                'api',
                'https://example.test/items',
                options={'timeout': 3.0},
                cfg=cfg,
                connector_obj=connector,
            )
        finally:
            monkeypatch.undo()

        assert result == {'ok': True}
        assert calls == [(cfg, connector, {'timeout': 3.0}, payload)]

    def test_extract_transform_then_return_when_no_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`run_pipeline` extracts, transforms, and returns data.
        """
        extract_calls: list[tuple[Any, Any, dict[str, Any]]] = []
        transform_calls: list[tuple[Any, Any]] = []

        def _extract(
            source_type: Any,
            source: Any,
            **kwargs: Any,
        ) -> dict[str, int]:
            extract_calls.append((source_type, source, kwargs))
            return {'value': 1}

        def _transform(
            data: Any,
            operations: Any,
        ) -> dict[str, int]:
            transform_calls.append((data, operations))
            return {'value': 2}

        monkeypatch.setattr(
            run_mod,
            'extract',
            _extract,
        )
        monkeypatch.setattr(
            run_mod,
            'transform',
            _transform,
        )

        ops: dict[str, Any] = {'map': {'value': 'value'}}
        result = run_mod.run_pipeline(
            source_type='file',
            source='/tmp/in.json',
            operations=ops,
            target_type=None,
        )

        assert result == {'value': 2}
        assert extract_calls == [
            ('file', '/tmp/in.json', {'file_format': None}),
        ]
        assert transform_calls == [({'value': 1}, ops)]

    def test_index_connectors_rejects_duplicates(self) -> None:
        """Test that connector indexing rejects duplicate names."""
        connectors = [
            SimpleNamespace(name='dup'),
            SimpleNamespace(name='dup'),
        ]
        with pytest.raises(
            ValueError,
            match='Duplicate source connector name',
        ):
            run_mod._index_connectors(connectors, label='source')

    def test_index_connectors_skips_missing_or_blank_names(self) -> None:
        """Test that connector indexing skips unnamed entries."""
        connectors = [
            SimpleNamespace(name='valid'),
            SimpleNamespace(name=''),
            SimpleNamespace(),
        ]

        indexed = run_mod._index_connectors(connectors, label='source')

        assert indexed == {'valid': connectors[0]}

    @pytest.mark.parametrize(
        ('connector_type', 'expected'),
        [
            ('file', True),
            (run_mod.DataConnectorType.FILE, True),
            ('api', False),
            (None, False),
        ],
    )
    def test_is_file_connector_type(
        self,
        connector_type: Any,
        expected: bool,
    ) -> None:
        """File-connector detection should accept both enum and string forms."""
        assert run_mod._is_file_connector_type(connector_type) is expected

    def test_resolve_job_connector_for_file_connector(
        self,
    ) -> None:
        """File connectors should resolve merged path, format, and options."""
        connector = SimpleNamespace(
            name='src',
            type='file',
            path='/tmp/default.json',
            format='json',
            options={'compression': 'gzip', 'path': '/ignored'},
        )

        result = run_mod._resolve_job_connector(
            {'src': connector},
            ref_name='src',
            label='source',
            overrides={
                'path': '/tmp/override.csv',
                'format': 'csv',
                'delimiter': ';',
            },
            missing_path_message='missing path',
        )

        assert result.connector_type == 'file'
        assert result.value == '/tmp/override.csv'
        assert result.file_format == 'csv'
        assert result.options == {
            'compression': 'gzip',
            'delimiter': ';',
        }
        assert result.connector_obj is connector

    def test_resolve_job_connector_for_non_file_connector(
        self,
    ) -> None:
        """Non-file connectors should preserve overrides and connection value."""
        connector = SimpleNamespace(
            name='api_src',
            type='api',
            connection_string='https://default.test/items',
        )

        result = run_mod._resolve_job_connector(
            {'api_src': connector},
            ref_name='api_src',
            label='source',
            overrides={
                'connection_string': 'https://override.test/items',
                'timeout': 5.0,
            },
            missing_path_message='missing path',
        )

        assert result.connector_type == 'api'
        assert result.value == 'https://override.test/items'
        assert result.file_format is None
        assert result.options == {
            'connection_string': 'https://override.test/items',
            'timeout': 5.0,
        }
        assert result.connector_obj is connector

    def test_run_treats_missing_transform_registry_as_noop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that missing transform registries do not break job runs."""
        job = _make_job(name='job', source='src', target='tgt')
        cfg = SimpleNamespace(
            jobs=[job],
            sources=[
                SimpleNamespace(
                    name='src',
                    type='file',
                    path='/tmp/in.json',
                    format='json',
                ),
            ],
            targets=[
                SimpleNamespace(
                    name='tgt',
                    type='file',
                    path='/tmp/out.json',
                    format='json',
                ),
            ],
            transforms=None,
            validations={},
        )
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: {'id': 1})
        monkeypatch.setattr(
            run_mod,
            'maybe_validate',
            lambda data, *_a, **_k: data,
        )
        transform_calls: list[Any] = []

        def _transform(data: Any, ops: Any) -> Any:
            transform_calls.append(ops)
            return data

        monkeypatch.setattr(run_mod, 'transform', _transform)
        monkeypatch.setattr(run_mod, 'load', lambda *_a, **_k: {'status': 'ok'})

        result = run_mod.run('job')

        assert result == {'status': 'ok'}
        assert transform_calls == []

    def test_run_uses_empty_rules_when_validation_registry_is_not_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that invalid validation registries degrade to empty rules."""
        job = _make_job(name='job', source='src', target='tgt')
        job.validate = SimpleNamespace(
            ruleset='default',
            severity='warn',
            phase='after_transform',
        )
        cfg = _base_config(
            job,
            SimpleNamespace(
                name='src',
                type='file',
                path='/tmp/in.json',
                format='json',
            ),
            SimpleNamespace(
                name='tgt',
                type='file',
                path='/tmp/out.json',
                format='json',
            ),
        )
        cfg.validations = ['invalid']
        monkeypatch.setattr(
            run_mod.Config,
            'from_yaml',
            lambda path, substitute=True: cfg,
        )
        monkeypatch.setattr(run_mod, 'extract', lambda *_a, **_k: {'id': 1})
        validation_calls: list[tuple[str, dict[str, Any]]] = []

        def _maybe_validate(
            data: Any,
            when: str,
            **kwargs: Any,
        ) -> Any:
            validation_calls.append((when, kwargs))
            return data

        monkeypatch.setattr(run_mod, 'maybe_validate', _maybe_validate)
        monkeypatch.setattr(run_mod, 'transform', lambda data, _ops: data)
        monkeypatch.setattr(run_mod, 'load', lambda *_a, **_k: {'status': 'ok'})

        result = run_mod.run('job')

        assert result == {'status': 'ok'}
        assert [when for when, _ in validation_calls] == [
            'before_transform',
            'after_transform',
        ]
        assert all(kwargs['enabled'] is True for _, kwargs in validation_calls)
        assert all(kwargs['rules'] == {} for _, kwargs in validation_calls)
        assert all(
            kwargs['phase'] == 'after_transform' for _, kwargs in validation_calls
        )
        assert all(kwargs['severity'] == 'warn' for _, kwargs in validation_calls)


class TestRunPipeline:
    """Unit tests for :func:`etlplus.ops.run.run_pipeline`."""

    def test_requires_record_payload_when_no_target(self) -> None:
        """
        Test that :func:`run_pipeline` enforces dict/list payloads when not
        loading.
        """
        with pytest.raises(
            TypeError,
            match='Expected data to be dict or list',
        ):
            run_mod.run_pipeline(source_type=None, source=cast(Any, 123))

    def test_requires_source_when_source_type_is_none(self) -> None:
        """
        Test that :func:`run_pipeline` requires a source payload when no
        *source_type* is provided.
        """
        with pytest.raises(
            ValueError,
            match='source or source_type is required',
        ):
            run_mod.run_pipeline(source_type=None, source=None)

    def test_requires_source_when_source_type_is_set(self) -> None:
        """
        Test that :func:`run_pipeline` requires a source payload when
        *source_type* is provided.
        """
        with pytest.raises(ValueError, match='source is required'):
            run_mod.run_pipeline(source_type='file', source=None)

    def test_requires_path_like_source_when_source_type_is_set(self) -> None:
        """Non-path payloads should be rejected before extract dispatch."""
        with pytest.raises(TypeError, match='source must be a path-like'):
            run_mod.run_pipeline(
                source_type='file',
                source=cast(Any, {'id': 1}),
            )

    def test_requires_target_when_target_type_is_set(self) -> None:
        """
        Test that :func:`run_pipeline` requires a target when *target_type* is
        provided.
        """
        with pytest.raises(ValueError, match='target is required'):
            run_mod.run_pipeline(
                source_type=None,
                source={'id': 1},
                target_type='file',
                target=None,
            )
