"""
:mod:`tests.unit.queue.test_u_queue` module.

Unit tests for :mod:`etlplus.queue` package exports.
"""

from __future__ import annotations

import pytest

import etlplus.queue as queue_pkg
from etlplus.queue import AmqpQueue
from etlplus.queue import AmqpQueueConfigDict
from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import AzureServiceBusQueueConfigDict
from etlplus.queue import GcpPubSubQueue
from etlplus.queue import GcpPubSubQueueConfigDict
from etlplus.queue import QueueConfig
from etlplus.queue import QueueConfigProtocol
from etlplus.queue import QueueLocation
from etlplus.queue import QueueService
from etlplus.queue import QueueType
from etlplus.queue import RedisQueue
from etlplus.queue import RedisQueueConfigDict
from etlplus.queue import SqsQueue
from etlplus.queue import SqsQueueConfigDict

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


QUEUE_EXPORTS = [
    ('AmqpQueue', AmqpQueue),
    ('AzureServiceBusQueue', AzureServiceBusQueue),
    ('GcpPubSubQueue', GcpPubSubQueue),
    ('QueueLocation', QueueLocation),
    ('RedisQueue', RedisQueue),
    ('SqsQueue', SqsQueue),
    ('QueueService', QueueService),
    ('QueueType', QueueType),
    ('QueueConfigProtocol', QueueConfigProtocol),
    ('QueueConfig', QueueConfig),
    ('AmqpQueueConfigDict', AmqpQueueConfigDict),
    ('AzureServiceBusQueueConfigDict', AzureServiceBusQueueConfigDict),
    ('GcpPubSubQueueConfigDict', GcpPubSubQueueConfigDict),
    ('RedisQueueConfigDict', RedisQueueConfigDict),
    ('SqsQueueConfigDict', SqsQueueConfigDict),
]


# SECTION: TESTS ============================================================ #


class TestQueuePackageExports:
    """Unit tests for package-level exports."""

    @pytest.mark.parametrize(('name', 'expected'), QUEUE_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(queue_pkg, name) == expected

    def test_expected_symbols(self) -> None:
        """Test that package facade preserves the documented export order."""
        assert queue_pkg.__all__ == [name for name, _value in QUEUE_EXPORTS]
