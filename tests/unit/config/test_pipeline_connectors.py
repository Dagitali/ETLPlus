"""
ETLPlus Pipeline Connector Tests
================================

Unit tests for the ETLPlus pipeline connector handling.
"""
from __future__ import annotations

from etlplus.config.connector import ConnectorApi
from etlplus.config.connector import ConnectorDb
from etlplus.config.connector import ConnectorFile
from etlplus.config.pipeline import _build_connectors


def test_build_connectors_skips_malformed_and_unsupported():
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
            123,  # non-dict, should be skipped
            {'name': 'weird', 'type': 'unknown'},  # unsupported -> skipped
            {'type': 'file'},  # missing name -> skipped
        ],
    }

    items = _build_connectors(raw, 'sources')
    # Expect only the three valid connectors constructed
    assert len(items) == 3
    assert any(isinstance(c, ConnectorFile) for c in items)
    assert any(isinstance(c, ConnectorApi) for c in items)
    assert any(isinstance(c, ConnectorDb) for c in items)
