"""
:mod:`tests.integration.test_i_run` module.

Validates :func:`run` orchestration end-to-end for service + endpoint URL
composition under a minimal pipeline wiring (file source → API target).

Notes
-----
- Ensures profile ``base_path`` is joined with endpoint path.
- Patches nothing network-related; uses real file source for realism.
- Asserts composed URL and capture of API load invocation via fixture.
"""

from __future__ import annotations

import csv
import importlib
from collections.abc import Callable
from io import BytesIO
from io import StringIO
from typing import Any

import pytest
from pytest import MonkeyPatch

from etlplus import Config
from etlplus.connector import ConnectorFile
from etlplus.connector import DataConnectorType
from etlplus.workflow import ExtractRef
from etlplus.workflow import JobConfig
from etlplus.workflow import LoadRef

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


run_mod = importlib.import_module('etlplus.ops.run')


# SECTION: TESTS ============================================================ #


def test_remote_file_pipeline_reads_and_writes_via_storage_backend(
    monkeypatch: MonkeyPatch,
) -> None:
    """Test a full remote file pipeline run with option forwarding."""
    core_mod = importlib.import_module('etlplus.file._core')

    remote_objects: dict[str, bytes] = {
        's3://bucket/input.csv': b'name|age\nAda|36\nGrace|47\n',
    }
    writes: list[tuple[str, bytes]] = []

    class CaptureUpload(BytesIO):
        """Capture uploaded remote content on close."""

        def __init__(self, uri: str) -> None:
            super().__init__()
            self._uri = uri

        def close(self) -> None:
            remote_objects[self._uri] = self.getvalue()
            writes.append((self._uri, self.getvalue()))
            super().close()

    class FakeBackend:
        """Minimal remote backend for end-to-end file pipeline tests."""

        def exists(self, location: object) -> bool:
            uri = getattr(location, 'raw', str(location))
            return uri in remote_objects

        def ensure_parent_dir(self, location: object) -> None:
            del location

        def open(
            self,
            location: object,
            mode: str = 'r',
            **kwargs: object,
        ) -> BytesIO:
            del kwargs
            uri = getattr(location, 'raw', str(location))
            if 'r' in mode:
                return BytesIO(remote_objects[uri])
            return CaptureUpload(uri)

    cfg = Config(
        sources=[
            ConnectorFile(
                name='remote_src',
                type=DataConnectorType.FILE,
                format='csv',
                path='s3://bucket/input.csv',
                options={'delimiter': ',', 'encoding': 'utf-8'},
            ),
        ],
        targets=[
            ConnectorFile(
                name='remote_tgt',
                type=DataConnectorType.FILE,
                format='csv',
                path='s3://bucket/output.csv',
                options={'delimiter': ',', 'encoding': 'utf-8'},
            ),
        ],
        jobs=[
            JobConfig(
                name='ship_remote',
                extract=ExtractRef(
                    source='remote_src',
                    options={'delimiter': '|'},
                ),
                load=LoadRef(
                    target='remote_tgt',
                    overrides={'delimiter': ';'},
                ),
            ),
        ],
    )

    monkeypatch.setattr(run_mod.Config, 'from_yaml', lambda *_a, **_k: cfg)
    monkeypatch.setattr(core_mod, 'get_backend', lambda _value: FakeBackend())

    result = run_mod.run('ship_remote')

    assert result == {
        'status': 'success',
        'message': 'Data loaded to s3://bucket/output.csv',
        'records': 2,
    }
    assert len(writes) == 1
    assert writes[0][0] == 's3://bucket/output.csv'
    decoded = writes[0][1].decode('utf-8')
    rows = list(csv.DictReader(StringIO(decoded), delimiter=';'))
    assert rows == [
        {'age': '36', 'name': 'Ada'},
        {'age': '47', 'name': 'Grace'},
    ]


@pytest.mark.parametrize(
    ('base_path', 'endpoint_path', 'expected_suffix'),
    [
        ('/v1', '/ingest', '/v1/ingest'),
        (None, '/bulk', '/bulk'),
    ],
    ids=['with-base-path', 'without-base-path'],
)
def test_target_service_endpoint_uses_base_path(
    monkeypatch: MonkeyPatch,
    capture_load_to_api: dict[str, Any],
    file_to_api_pipeline_factory: Callable[..., Config],
    base_url: str,
    base_path: str | None,
    endpoint_path: str,
    expected_suffix: str,
):
    """Test composed API URLs across optional *base_path* values."""

    cfg = file_to_api_pipeline_factory(
        base_path=base_path,
        endpoint_path=endpoint_path,
        headers={'Content-Type': 'application/json'},
    )
    monkeypatch.setattr(run_mod.Config, 'from_yaml', lambda *_a, **_k: cfg)

    result = run_mod.run('send')

    assert result.get('status') in {'ok', 'success'}
    assert capture_load_to_api['url'] == f'{base_url}{expected_suffix}'

    headers = capture_load_to_api.get('headers') or {}
    assert headers.get('Content-Type') == 'application/json'
