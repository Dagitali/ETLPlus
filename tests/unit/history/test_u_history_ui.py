"""Unit tests for the local ETLPlus run-history UI helpers."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from etlplus.history._ui import build_history_ui_handler
from etlplus.history._ui import build_snapshot
from etlplus.history._ui import render_html
from etlplus.history._ui import serve_history_ui

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryUiSnapshot:
    """Unit tests for the read-only history UI snapshot builders."""

    def test_build_snapshot_uses_existing_history_read_path(
        self,
        monkeypatch,
    ) -> None:
        """Snapshot building should query run and job history through HistoryView."""
        calls: list[dict[str, object]] = []

        def fake_load_records(**kwargs):
            calls.append(kwargs)
            if kwargs['level'] == 'run':
                return [{'run_id': 'run-1', 'status': 'succeeded'}]
            return [{'run_id': 'run-1', 'job_name': 'seed', 'status': 'failed'}]

        monkeypatch.setattr(
            'etlplus.history._ui.HistoryView.load_records',
            fake_load_records,
        )

        snapshot = build_snapshot(limit=7)

        assert calls == [
            {'raw': False, 'level': 'run', 'limit': 7},
            {'raw': False, 'level': 'job', 'limit': 7},
        ]
        assert snapshot['summary'] == {
            'failed_jobs': 1,
            'failed_runs': 0,
            'job_rows': 1,
            'run_rows': 1,
            'succeeded_jobs': 0,
            'succeeded_runs': 1,
        }

    def test_render_html_includes_summary_and_tables(self) -> None:
        """Rendered HTML should expose summary cards and both history tables."""
        html_text = render_html(
            {
                'summary': {
                    'failed_jobs': 1,
                    'failed_runs': 2,
                    'job_rows': 4,
                    'run_rows': 3,
                    'succeeded_jobs': 3,
                    'succeeded_runs': 1,
                },
                'runs': [
                    {
                        'run_id': 'run-1',
                        'status': 'succeeded',
                        'job_name': None,
                        'pipeline_name': 'pipeline-a',
                        'started_at': '2026-05-14T00:00:00Z',
                        'finished_at': '2026-05-14T00:01:00Z',
                        'duration_ms': 60000,
                    },
                ],
                'jobs': [
                    {
                        'run_id': 'run-1',
                        'sequence_index': 0,
                        'job_name': 'seed',
                        'status': 'failed',
                        'result_status': 'failed',
                        'pipeline_name': 'pipeline-a',
                        'started_at': '2026-05-14T00:00:00Z',
                        'finished_at': '2026-05-14T00:00:05Z',
                        'duration_ms': 5000,
                    },
                ],
            },
            refresh_seconds=3,
        )

        assert 'ETLPlus local history UI' in html_text
        assert 'Recent runs without an external service' in html_text
        assert 'Visible Runs' in html_text
        assert 'Latest Runs' in html_text
        assert 'Latest Jobs' in html_text
        assert 'run-1' in html_text
        assert 'seed' in html_text
        assert 'http-equiv="refresh" content="3"' in html_text

    def test_render_html_omits_refresh_meta_when_refresh_disabled(self) -> None:
        """Rendered HTML should omit refresh metadata when refresh is disabled."""
        html_text = render_html(
            {'jobs': [], 'runs': [], 'summary': {}},
            refresh_seconds=0,
        )

        assert 'http-equiv="refresh"' not in html_text


class TestHistoryUiHttpHandler:
    """Unit tests for the local history UI HTTP endpoints."""

    def test_handler_serves_html_json_and_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Configured handler should serve the documented read-only endpoints."""
        monkeypatch.setattr(
            'etlplus.history._ui.build_snapshot',
            lambda *, limit: {
                'jobs': [],
                'runs': [{'run_id': 'run-1', 'status': 'succeeded'}],
                'summary': {
                    'failed_jobs': 0,
                    'failed_runs': 0,
                    'job_rows': 0,
                    'run_rows': 1,
                    'succeeded_jobs': 0,
                    'succeeded_runs': 1,
                },
            },
        )
        handler = build_history_ui_handler(limit=5, refresh_seconds=0)
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            host, port = server.server_address
            base_url = f'http://{host}:{port}'

            with urllib.request.urlopen(f'{base_url}/', timeout=5) as response:
                html_body = response.read().decode('utf-8')
                assert response.status == 200
                assert response.headers['Content-Type'] == ('text/html; charset=utf-8')
                assert 'ETLPlus local history UI' in html_body
                assert 'run-1' in html_body

            with urllib.request.urlopen(
                f'{base_url}/snapshot.json',
                timeout=5,
            ) as response:
                payload = json.loads(response.read().decode('utf-8'))
                assert response.status == 200
                assert response.headers['Content-Type'] == (
                    'application/json; charset=utf-8'
                )
                assert payload['runs'][0]['run_id'] == 'run-1'

            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(f'{base_url}/missing', timeout=5)
            assert exc_info.value.code == 404
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_serve_history_ui_opens_browser_for_loopback_host(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Serving on the default loopback host should open the local UI URL."""
        closed = False
        opened_urls: list[str] = []

        class _FakeServer:
            def __init__(self, address: tuple[str, int], _handler: object) -> None:
                self.address = address

            def serve_forever(self) -> None:
                return None

            def server_close(self) -> None:
                nonlocal closed
                closed = True

        monkeypatch.setattr('etlplus.history._ui.ThreadingHTTPServer', _FakeServer)
        monkeypatch.setattr(
            'etlplus.history._ui.webbrowser.open',
            lambda url: opened_urls.append(url),
        )

        exit_code = serve_history_ui(port=8766)

        captured = capsys.readouterr()
        assert exit_code == 0
        assert opened_urls == ['http://127.0.0.1:8766/']
        assert 'ETLPlus History UI listening on http://127.0.0.1:8766/' in captured.out
        assert captured.err == ''
        assert closed is True

    def test_serve_history_ui_warns_when_bound_to_all_interfaces(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Serving on ``0.0.0.0`` should warn about exposed local history."""

        class _FakeServer:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

            def serve_forever(self) -> None:
                raise KeyboardInterrupt

            def server_close(self) -> None:
                pass

        monkeypatch.setattr('etlplus.history._ui.ThreadingHTTPServer', _FakeServer)

        exit_code = serve_history_ui(host='0.0.0.0', open_browser=False)

        assert exit_code == 0
        assert 'bound to 0.0.0.0' in capsys.readouterr().err
