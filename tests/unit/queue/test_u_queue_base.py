"""
:mod:`tests.unit.queue.test_u_queue_base` module.

Unit tests for :mod:`etlplus.queue._base`.
"""

from __future__ import annotations

import pytest

from etlplus.queue import QueueService
from etlplus.queue._base import ProviderQueueConfigMixin

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=protected-access

# SECTION: HELPERS ========================================================== #


class _QueueConfig(ProviderQueueConfigMixin):
    """Minimal concrete queue config for mixin behavior tests."""

    _option_fields = ('name', 'endpoint', 'optional')

    def __init__(self) -> None:
        self.service = QueueService.REDIS
        self.options = {
            'service': 'stale',
            'name': 'stale',
            'endpoint': 'stale',
            'kept': 'value',
        }
        self.name = 'jobs'
        self.endpoint = 'redis://localhost'
        self.optional = None


# SECTION: TESTS ============================================================ #


class TestProviderQueueConfigMixin:
    """Unit tests for :class:`ProviderQueueConfigMixin`."""

    def test_common_fields_rejects_blank_name(self) -> None:
        """Blank queue names should be rejected after stripping whitespace."""
        with pytest.raises(TypeError, match='Queue requires a "name"'):
            ProviderQueueConfigMixin._common_fields(
                {'name': '   '},
                label='Queue',
            )

    def test_common_fields_trims_name_and_parses_options(self) -> None:
        """Common queue fields should normalize names and options mappings."""
        fields = ProviderQueueConfigMixin._common_fields(
            {
                'name': '  jobs  ',
                'settings': {'durable': True},
            },
            label='Queue',
            options_field='settings',
            options_key='settings',
        )

        assert fields == {'name': 'jobs', 'settings': {'durable': True}}

    @pytest.mark.parametrize(
        ('obj', 'field', 'alias', 'expected'),
        [
            ({'endpoint': '   '}, 'endpoint', None, None),
            (
                {'endpoint': '  redis://localhost  '},
                'endpoint',
                None,
                'redis://localhost',
            ),
            (
                {'queue_name': '  primary  ', 'queue': 'alias'},
                'queue_name',
                'queue',
                'primary',
            ),
            ({'queue': '  jobs  '}, 'queue_name', 'queue', 'jobs'),
        ],
    )
    def test_optional_str_normalizes_values(
        self,
        obj: dict[str, object],
        field: str,
        alias: str | None,
        expected: str | None,
    ) -> None:
        """Optional string fields should trim values and support aliases."""
        assert (
            ProviderQueueConfigMixin._optional_str(obj, field, alias=alias) == expected
        )

    def test_to_connector_options_does_not_mutate_provider_options(self) -> None:
        """Connector option serialization should not mutate stored options."""
        queue = _QueueConfig()

        queue.to_connector_options()

        assert queue.options == {
            'service': 'stale',
            'name': 'stale',
            'endpoint': 'stale',
            'kept': 'value',
        }

    def test_to_connector_options_overlays_modeled_fields(self) -> None:
        """Modeled fields should override duplicate raw provider options."""
        queue = _QueueConfig()

        assert queue.to_connector_options() == {
            'service': 'redis',
            'name': 'jobs',
            'endpoint': 'redis://localhost',
            'kept': 'value',
        }
