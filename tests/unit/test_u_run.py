"""
:mod:`tests.unit.test_u_run` module.

Unit tests for :mod:`etlplus.run`.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any
from typing import ClassVar
from typing import Self

import pytest

run_mod = importlib.import_module('etlplus.run')

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


def _make_job(
    *,
    name: str,
    source: str,
    target: str,
    options: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        extract=SimpleNamespace(source=source, options=options or {}),
        transform=SimpleNamespace(pipeline='noop'),
        load=SimpleNamespace(target=target, overrides=None),
        validate=None,
    )


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


# SECTION: TESTS ============================================================ #


class TestRun:
    """Unit test suite for :func:`etlplus.run.run`."""

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
            run_mod,
            'load_pipeline_config',
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
            run_mod,
            'compose_api_request_env',
            lambda cfg_obj, source_obj, opts: req_env,
        )

        class DummyClient:
            """Dummy EndpointClient for testing purposes."""

            instances: ClassVar[list[Self]] = []

            def __init__(self, **kwargs: Any) -> None:
                self.kwargs = kwargs
                DummyClient.instances.append(self)

        monkeypatch.setattr(run_mod, 'EndpointClient', DummyClient)

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

        monkeypatch.setattr(run_mod, 'paginate_with_client', _capture_paginate)

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
            run_mod,
            'compose_api_target_env',
            lambda cfg_obj, target_obj, overrides: target_env,
        )

        load_calls: list[tuple] = []

        def _capture_load(
            data: Any,
            connector: str,
            url: str,
            **kwargs: Any,
        ) -> dict[str, bool]:
            load_calls.append((data, connector, url, kwargs))
            return {'ok': True}

        monkeypatch.setattr(run_mod, 'load', _capture_load)

        result = run_mod.run('api_job')

        assert DummyClient.instances
        assert paginate_calls[0]['endpoint_key'] == 'users'
        assert paginate_calls[0]['params'] == {'limit': 5}
        assert load_calls[0][1] == 'api'
        assert load_calls[0][2] == 'https://sink.example.com'
        assert load_calls[0][3]['method'] == 'put'
        assert result == {'ok': True}

    def test_file_to_file_pipeline(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test a file-to-file ETL pipeline execution."""
        # pylint: disable=unused-argument
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
            run_mod,
            'load_pipeline_config',
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

    @pytest.mark.parametrize(
        'cfg',
        [
            SimpleNamespace(
                jobs=[],
                sources=[],
                targets=[],
                transforms={},
                validations={},
            ),
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
        ],
        ids=['no-jobs', 'different-job'],
    )
    def test_job_not_found_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cfg: Any,
    ) -> None:
        """Test that requesting a missing job raises :class:`ValueError`."""
        monkeypatch.setattr(
            run_mod,
            'load_pipeline_config',
            lambda path, substitute=True: cfg,
        )

        with pytest.raises(ValueError, match='Job not found'):
            run_mod.run('missing')
