"""
``tests.unit.config.test_pipeline_connectors`` module.


Unit tests for the ETLPlus pipeline connector handling.
"""
from __future__ import annotations

from etlplus.config.connector import ConnectorApi
from etlplus.config.connector import ConnectorDb
from etlplus.config.connector import ConnectorFile
from etlplus.config.pipeline import _build_connectors


# SECTION: TESTS ============================================================ #


class TestPipelineConnectors:
    def test_build_connectors_skips_malformed_and_unsupported(
        self,
    ) -> None:  # noqa: D401
        raw = {
            'sources': [
                {'name': 'csv_in', 'type': 'file', 'path': '/tmp/in.csv'},
                {
                    'name': 'service_in',
                    'type': 'api',
                    'api': 'github',
                    'endpoint': 'issues',
                },
                {'name': 'analytics', 'type': 'database', 'table': 'events'},
                123,  # Skip non-dict.
                {'name': 'weird', 'type': 'unknown'},  # Skip Unsupported.
                {'type': 'file'},  # Skip missing name.
            ],
        }

        items = _build_connectors(raw, 'sources')

        # Expect only the three valid connectors constructed.
        assert len(items) == 3
        assert any(isinstance(c, ConnectorFile) for c in items)
        assert any(isinstance(c, ConnectorApi) for c in items)
        assert any(isinstance(c, ConnectorDb) for c in items)

    def test_build_connectors_for_targets_key(self):  # noqa: D401
        raw = {
            'targets': [
                {'name': 'csv_out', 'type': 'file', 'path': '/tmp/out.csv'},
                {'name': 'sink', 'type': 'database', 'table': 'events_out'},
                {
                    'name': 'svc',
                    'type': 'api',
                    'api': 'hub',
                    'endpoint': 'post',
                },
                {'name': 'bad', 'type': 'unknown'},   # Skipped.
            ],
        }

        items = _build_connectors(raw, 'targets')
        assert len(items) == 3
        assert any(isinstance(c, ConnectorFile) for c in items)
        assert any(isinstance(c, ConnectorDb) for c in items)
        assert any(isinstance(c, ConnectorApi) for c in items)
