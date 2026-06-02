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

    def test_optional_str_treats_blank_values_as_absent(self) -> None:
        """Blank optional strings should normalize to ``None``."""
        assert (
            ProviderQueueConfigMixin._optional_str({'endpoint': '   '}, 'endpoint')
            is None
        )

    def test_optional_str_trims_preferred_field(self) -> None:
        """Optional string fields should return trimmed preferred values."""
        assert (
            ProviderQueueConfigMixin._optional_str(
                {'endpoint': '  redis://localhost  '},
                'endpoint',
            )
            == 'redis://localhost'
        )

    def test_optional_str_prefers_primary_field_over_alias(self) -> None:
        """Primary field names should take precedence over aliases."""
        assert (
            ProviderQueueConfigMixin._optional_str(
                {'queue_name': '  primary  ', 'queue': 'alias'},
                'queue_name',
                alias='queue',
            )
            == 'primary'
        )

    def test_optional_str_uses_alias_when_preferred_field_is_absent(self) -> None:
        """Optional string fields should support provider-specific aliases."""
        assert (
            ProviderQueueConfigMixin._optional_str(
                {'queue': '  jobs  '},
                'queue_name',
                alias='queue',
            )
            == 'jobs'
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
