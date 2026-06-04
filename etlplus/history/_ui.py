"""Internal local web UI helpers for persisted ETLPlus run history."""

from __future__ import annotations

import html
import json
import webbrowser
from collections import Counter
from collections.abc import Sequence
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from typing import Any

from etlplus.cli._handlers._history_view import HistoryView

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'DEFAULT_UI_HOST',
    'DEFAULT_UI_LIMIT',
    'DEFAULT_UI_PORT',
    'DEFAULT_UI_REFRESH_SECONDS',
    'build_snapshot',
    'render_html',
    'serve_history_ui',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_UI_HOST = '127.0.0.1'
DEFAULT_UI_LIMIT = 20
DEFAULT_UI_PORT = 8765
DEFAULT_UI_REFRESH_SECONDS = 5


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _build_summary(
    *,
    jobs: Sequence[dict[str, Any]],
    runs: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Return one summary payload for the visible run and job slices."""
    run_counts = Counter(
        str(status) for record in runs if (status := record.get('status')) is not None
    )
    job_counts = Counter(
        str(status) for record in jobs if (status := record.get('status')) is not None
    )
    return {
        'failed_jobs': job_counts.get('failed', 0),
        'failed_runs': run_counts.get('failed', 0),
        'job_rows': len(jobs),
        'run_rows': len(runs),
        'succeeded_jobs': job_counts.get('succeeded', 0),
        'succeeded_runs': run_counts.get('succeeded', 0),
    }


def _html_document(
    *,
    body: str,
    refresh_seconds: int,
) -> str:
    """Wrap one history UI body fragment in a complete HTML document."""
    refresh_meta = (
        f'<meta http-equiv="refresh" content="{refresh_seconds}">'
        if refresh_seconds > 0
        else ''
    )
    return (
        '<!doctype html>'
        '<html lang="en">'
        '<head>'
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'{refresh_meta}'
        '<title>ETLPlus History UI</title>'
        '<style>'
        ':root { color-scheme: light; }'
        'body { margin: 0; font-family: Georgia, "Iowan Old Style", serif; '
        'background: linear-gradient(180deg, #f6f0e8 0%, #f2f7fb 100%); '
        'color: #16324f; }'
        '.shell { max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }'
        '.hero { display: grid; gap: 12px; margin-bottom: 24px; }'
        '.eyebrow { letter-spacing: 0.08em; text-transform: uppercase; '
        'font: 600 12px/1.4 ui-monospace, Menlo, monospace; '
        'color: #8c5e34; }'
        'h1 { margin: 0; font-size: clamp(2rem, 4vw, 3.5rem); line-height: 1; }'
        '.lead { max-width: 70ch; margin: 0; font-size: 1rem; color: #35516d; }'
        '.cards { display: grid; grid-template-columns: repeat(auto-fit, '
        'minmax(160px, 1fr)); gap: 12px; margin: 24px 0; }'
        '.card { background: rgba(255,255,255,0.78); '
        'border: 1px solid #d4dfeb; border-radius: 16px; padding: 16px; '
        'box-shadow: 0 10px 30px rgba(22,50,79,0.08); }'
        '.card-label { display: block; font: 600 12px/1.4 ui-monospace, '
        'Menlo, monospace; color: #6a7f95; text-transform: uppercase; }'
        '.card-value { display: block; margin-top: 8px; font-size: 2rem; '
        'line-height: 1; }'
        '.grid { display: grid; gap: 18px; grid-template-columns: '
        'repeat(auto-fit, minmax(320px, 1fr)); }'
        '.panel { background: rgba(255,255,255,0.82); '
        'border: 1px solid #d4dfeb; border-radius: 18px; padding: 18px; '
        'box-shadow: 0 10px 30px rgba(22,50,79,0.08); overflow: auto; }'
        '.panel h2 { margin: 0 0 12px; font-size: 1.2rem; }'
        'table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }'
        'th, td { padding: 10px 8px; border-bottom: 1px solid #e5edf5; '
        'text-align: left; vertical-align: top; }'
        'th { font: 600 12px/1.4 ui-monospace, Menlo, monospace; '
        'color: #6a7f95; text-transform: uppercase; }'
        'tbody tr:last-child td { border-bottom: none; }'
        '.pill { display: inline-block; border-radius: 999px; '
        'padding: 2px 10px; font: 600 12px/1.4 ui-monospace, Menlo, monospace; }'
        '.status-succeeded { background: #dff6e8; color: #136f3c; }'
        '.status-failed { background: #fde7e4; color: #b03a2e; }'
        '.status-running { background: #fff1cc; color: #8a6200; }'
        '.footer { margin-top: 18px; color: #6a7f95; font-size: 0.92rem; }'
        '@media (max-width: 700px) { .shell { padding: 20px 14px 36px; } }'
        '</style>'
        '</head>'
        f'<body>{body}</body>'
        '</html>'
    )


def _render_status(
    value: object,
) -> str:
    """Return one styled status pill for the UI tables."""
    text = '' if value is None else str(value)
    status_class = f'status-{text}' if text else ''
    return f'<span class="pill {status_class}">{html.escape(text or "-")}</span>'


def _render_table(
    *,
    columns: Sequence[str],
    rows: Sequence[dict[str, Any]],
) -> str:
    """Render one HTML table for a history row slice."""
    header_html = ''.join(f'<th>{html.escape(column)}</th>' for column in columns)
    if not rows:
        return (
            '<table><thead><tr>'
            f'{header_html}'
            '</tr></thead><tbody><tr><td colspan="'
            f'{len(columns)}">No records found.</td></tr></tbody></table>'
        )

    body_rows: list[str] = []
    for row in rows:
        cells: list[str] = []
        for column in columns:
            value = row.get(column)
            rendered = (
                _render_status(value)
                if column == 'status'
                else html.escape(
                    '' if value is None else str(value),
                )
            )
            cells.append(f'<td>{rendered or "-"}</td>')
        body_rows.append(f'<tr>{"".join(cells)}</tr>')
    return (
        '<table><thead><tr>'
        f'{header_html}'
        '</tr></thead><tbody>'
        f'{"".join(body_rows)}'
        '</tbody></table>'
    )


# SECTION: FUNCTIONS ======================================================== #


def build_snapshot(
    *,
    limit: int = DEFAULT_UI_LIMIT,
) -> dict[str, Any]:
    """Return one read-only snapshot payload for the local history UI."""
    runs = HistoryView.load_records(raw=False, level='run', limit=limit)
    jobs = HistoryView.load_records(raw=False, level='job', limit=limit)
    return {
        'jobs': jobs,
        'runs': runs,
        'summary': _build_summary(jobs=jobs, runs=runs),
    }


def render_html(
    snapshot: dict[str, Any],
    *,
    refresh_seconds: int = DEFAULT_UI_REFRESH_SECONDS,
) -> str:
    """Render one HTML page for the local run-history UI."""
    summary = snapshot.get('summary', {}) if isinstance(snapshot, dict) else {}
    run_table = _render_table(
        columns=HistoryView.table_columns('run'),
        rows=snapshot.get('runs', []),
    )
    job_table = _render_table(
        columns=HistoryView.table_columns('job'),
        rows=snapshot.get('jobs', []),
    )
    cards = ''.join(
        (
            '<article class="card">'
            f'<span class="card-label">{html.escape(label)}</span>'
            f'<span class="card-value">{html.escape(str(value))}</span>'
            '</article>'
        )
        for label, value in (
            ('Visible Runs', summary.get('run_rows', 0)),
            ('Succeeded Runs', summary.get('succeeded_runs', 0)),
            ('Failed Runs', summary.get('failed_runs', 0)),
            ('Visible Jobs', summary.get('job_rows', 0)),
            ('Succeeded Jobs', summary.get('succeeded_jobs', 0)),
            ('Failed Jobs', summary.get('failed_jobs', 0)),
        )
    )
    body = (
        '<main class="shell">'
        '<section class="hero">'
        '<span class="eyebrow">ETLPlus local history UI</span>'
        '<h1>Recent runs without an external service</h1>'
        '<p class="lead">'
        'This read-only view is rendered from the same persisted local history '
        'store used by the stable history, status, log, and report commands.'
        '</p>'
        '</section>'
        f'<section class="cards">{cards}</section>'
        '<section class="grid">'
        '<article class="panel"><h2>Latest Runs</h2>'
        f'{run_table}'
        '</article>'
        '<article class="panel"><h2>Latest Jobs</h2>'
        f'{job_table}'
        '</article>'
        '</section>'
        '<p class="footer">'
        'Auto-refresh uses simple page reloads. '
        'Use /snapshot.json for the raw snapshot payload.'
        '</p>'
        '</main>'
    )
    return _html_document(body=body, refresh_seconds=refresh_seconds)


def serve_history_ui(
    *,
    host: str = DEFAULT_UI_HOST,
    port: int = DEFAULT_UI_PORT,
    limit: int = DEFAULT_UI_LIMIT,
    refresh_seconds: int = DEFAULT_UI_REFRESH_SECONDS,
    open_browser: bool = True,
) -> int:
    """
    Run the local ETLPlus history UI HTTP server.

    Parameters
    ----------
    host : str, optional
        Host interface for the local web UI.
    port : int, optional
        TCP port for the local web UI.
    limit : int, optional
        Maximum number of run and job history rows to show.
    refresh_seconds : int, optional
        Page refresh interval in seconds. Use ``0`` to disable refresh.
    open_browser : bool, optional
        Whether to open the default browser automatically.

    Returns
    -------
    int
        A conventional POSIX exit code: zero on success, non-zero on error.
    """

    class _Handler(BaseHTTPRequestHandler):
        """Serve the HTML shell and JSON snapshot endpoints."""

        # The stdlib handler API requires the non-snake-case ``do_GET`` name.
        # pylint: disable=invalid-name
        def do_GET(self) -> None:
            """Serve the HTML UI shell or the raw JSON snapshot endpoint."""
            if self.path == '/snapshot.json':
                payload = json.dumps(build_snapshot(limit=limit), indent=2)
                body = payload.encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path not in {'/', '/index.html'}:
                self.send_error(404)
                return

            rendered = render_html(
                build_snapshot(limit=limit),
                refresh_seconds=max(refresh_seconds, 0),
            )
            body = rendered.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer((host, port), _Handler)
    url = f'http://{host}:{port}/'
    print(f'ETLPlus History UI listening on {url}')
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
