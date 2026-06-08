"""
:mod:`tests.unit.queue.test_u_queue_options` module.

Unit tests for shared queue option serialization behavior.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from etlplus.queue import AmqpQueue
from etlplus.queue import AwsSqsQueue
from etlplus.queue import AzureServiceBusQueue
from etlplus.queue import GcpPubSubQueue
from etlplus.queue import QueueConfig
from etlplus.queue import RedisQueue
from etlplus.utils._types import StrAnyMap

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestQueueOptions:
    """Unit tests for shared queue option serialization."""

    @pytest.mark.parametrize(
        ('factory', 'target_payload', 'match'),
        [
            (
                AmqpQueue.from_obj,
                {'url': 'amqp://guest:guest@localhost:5672/%2f'},
                'AmqpQueue requires a "name"',
            ),
            (
                AwsSqsQueue.from_obj,
                {'queue_type': 'fifo'},
                'AwsSqsQueue requires a "name"',
            ),
            (
                AzureServiceBusQueue.from_obj,
                {'queue_name': 'orders-in'},
                'AzureServiceBusQueue requires a "name"',
            ),
            (
                GcpPubSubQueue.from_obj,
                {'project': 'example-project', 'subscription': 'etlplus'},
                'GcpPubSubQueue requires a "name"',
            ),
            (
                RedisQueue.from_obj,
                {'url': 'redis://localhost:6379/0'},
                'RedisQueue requires a "name"',
            ),
        ],
    )
    @pytest.mark.parametrize(
        'name_payload',
        [
            pytest.param({}, id='missing-name'),
            pytest.param({'name': '   '}, id='blank-name'),
        ],
    )
    def test_from_obj_requires_name(
        self,
        factory: Callable[[StrAnyMap], QueueConfig],
        target_payload: StrAnyMap,
        match: str,
        name_payload: StrAnyMap,
    ) -> None:
        """Test that provider queue metadata requires a nonblank name."""
        with pytest.raises(TypeError, match=match):
            factory(name_payload | target_payload)

    @pytest.mark.parametrize(
        ('factory', 'payload', 'expected'),
        [
            (
                AmqpQueue.from_obj,
                {
                    'name': 'orders',
                    'host': 'localhost',
                    'options': {
                        'service': 'wrong',
                        'host': 'stale',
                    },
                },
                {
                    'service': 'amqp',
                    'host': 'localhost',
                },
            ),
            (
                AwsSqsQueue.from_obj,
                {
                    'name': 'events',
                    'region': 'us-east-1',
                    'attributes': {
                        'service': 'wrong',
                        'queue_type': 'wrong',
                        'queue_name': 'stale',
                        'region': 'stale',
                    },
                },
                {
                    'service': 'aws-sqs',
                    'queue_type': 'standard',
                    'queue_name': 'events',
                    'region': 'us-east-1',
                },
            ),
            (
                AzureServiceBusQueue.from_obj,
                {
                    'name': 'orders',
                    'queue_name': 'orders',
                    'options': {
                        'service': 'wrong',
                        'queue_name': 'stale',
                    },
                },
                {
                    'service': 'azure-service-bus',
                    'queue_name': 'orders',
                },
            ),
            (
                GcpPubSubQueue.from_obj,
                {
                    'name': 'orders',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                    'options': {
                        'service': 'wrong',
                        'project': 'stale',
                        'topic': 'stale',
                    },
                },
                {
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                },
            ),
            (
                RedisQueue.from_obj,
                {
                    'name': 'orders',
                    'key': 'events',
                    'database': 1,
                    'options': {
                        'service': 'wrong',
                        'key': 'stale',
                        'database': 0,
                    },
                },
                {
                    'service': 'redis',
                    'key': 'events',
                    'database': 1,
                },
            ),
        ],
    )
    def test_modeled_fields_override_provider_options(
        self,
        factory: Callable[[StrAnyMap], QueueConfig],
        payload: StrAnyMap,
        expected: dict[str, object],
    ) -> None:
        """Test top-level queue fields take precedence over duplicate options."""
        assert factory(payload).to_connector_options() == expected

    @pytest.mark.parametrize(
        ('factory', 'payload', 'expected'),
        [
            (
                AmqpQueue.from_obj,
                {'name': 'orders', 'host': 'localhost'},
                {
                    'service': 'amqp',
                    'host': 'localhost',
                },
            ),
            (
                AwsSqsQueue.from_obj,
                {'name': 'events'},
                {
                    'service': 'aws-sqs',
                    'queue_type': 'standard',
                    'queue_name': 'events',
                },
            ),
            (
                AzureServiceBusQueue.from_obj,
                {'name': 'orders', 'queue': 'orders-in'},
                {
                    'service': 'azure-service-bus',
                    'queue_name': 'orders-in',
                },
            ),
            (
                GcpPubSubQueue.from_obj,
                {
                    'name': 'orders',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                },
                {
                    'service': 'gcp-pubsub',
                    'project': 'example-project',
                    'topic': 'orders-topic',
                },
            ),
            (
                RedisQueue.from_obj,
                {'name': 'orders'},
                {'service': 'redis'},
            ),
        ],
    )
    def test_to_connector_options_omits_empty_optional_fields(
        self,
        factory: Callable[[StrAnyMap], QueueConfig],
        payload: StrAnyMap,
        expected: dict[str, object],
    ) -> None:
        """Test empty optional provider metadata does not appear in options."""
        assert factory(payload).to_connector_options() == expected
