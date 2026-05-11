# `etlplus.queue` Subpackage

Documentation for the `etlplus.queue` subpackage: queue metadata helpers for message-oriented ETL
sources and targets.

- Models queue configuration metadata separately from runtime queue clients
- Defines queue service and queue type enums used by queue connectors
- Includes AWS SQS standard and FIFO queue metadata validation
- Includes config-only helpers for Azure Service Bus, Google Cloud Pub/Sub, AMQP/RabbitMQ, and Redis
- Parses queue URI references through `QueueLocation`

Runtime queue clients are intentionally outside this package for now. Queue-backed pipeline
endpoints are represented through `etlplus.connector.ConnectorQueue`, which can be converted into
provider-specific queue config objects with `ConnectorQueue.to_queue_config()`.

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.queue` Subpackage](#etlplusqueue-subpackage)
  - [Relationship to `etlplus.connector`](#relationship-to-etlplusconnector)
  - [Public API](#public-api)
  - [Supported Queue Services](#supported-queue-services)
  - [AWS SQS Metadata](#aws-sqs-metadata)
  - [Queue Locations](#queue-locations)
  - [Dependency Extras](#dependency-extras)
  - [Example](#example)
  - [See Also](#see-also)

## Relationship to `etlplus.connector`

`etlplus.queue` describes queue endpoints. `etlplus.connector` describes how those endpoints appear
in pipeline configuration.

Use `ConnectorQueue` for connector-level metadata such as connector name, connector type, queue
service, queue name, region, URL, and provider-specific options. Use
`ConnectorQueue.to_queue_config()` when code needs a provider-specific queue metadata object such as
`SqsQueue` or `AzureServiceBusQueue`.

## Public API

- `AmqpQueue`: AMQP/RabbitMQ queue metadata.
- `AzureServiceBusQueue`: Azure Service Bus queue, topic, and subscription metadata.
- `GcpPubSubQueue`: Google Cloud Pub/Sub topic and subscription metadata.
- `QueueConfig`: Type alias for supported provider-specific queue config objects.
- `QueueConfigProtocol`: Shared protocol for queue config objects.
- `QueueLocation.from_value(value)`: Parse a queue URI into normalized service, authority, and path
  parts.
- `QueueService`: Queue service enum with aliases for supported providers.
- `QueueType`: Queue type enum for standard and FIFO queue semantics.
- `RedisQueue`: Redis queue-like workflow metadata.
- `SqsQueue`: AWS SQS queue metadata with standard and FIFO validation.

## Supported Queue Services

| Service             | Enum value          | Runtime package          | Extra             |
|---------------------|---------------------|--------------------------|-------------------|
| AWS SQS             | `aws-sqs`           | `boto3`                  | `queue-aws`       |
| Azure Service Bus   | `azure-service-bus` | `azure-servicebus`       | `queue-azure`     |
| Google Pub/Sub      | `gcp-pubsub`        | `google-cloud-pubsub`    | `queue-gcp`       |
| AMQP/RabbitMQ       | `amqp`              | `pika`                   | `queue-amqp`      |
| Redis               | `redis`             | `redis`                  | `queue-redis`     |

The aggregate `queue` and `queue-all` extras include all provider packages listed above.

## AWS SQS Metadata

`SqsQueue` supports standard and FIFO queue metadata, including:

- Queue name, URL, region, and queue type
- Delay seconds, visibility timeout, wait time seconds, message retention, and maximum message count
- Dead-letter queue ARN
- FIFO-only content-based deduplication, message group ID, and deduplication ID

FIFO queues must use the `.fifo` suffix. Standard queues reject FIFO-only fields so that invalid SQS
metadata is caught before runtime integration code consumes it.

## Queue Locations

Use `QueueLocation` when a queue endpoint is expressed as a URI:

```python
from etlplus.queue import QueueLocation

location = QueueLocation.from_value('aws-sqs://us-east-1/events.fifo')

assert location.service.value == 'aws-sqs'
assert location.authority == 'us-east-1'
assert location.path == 'events.fifo'
```

Common URI forms include:

- `aws-sqs://us-east-1/events.fifo`
- `sqs://us-east-1/events`
- `redis://localhost:6379/0/events`

## Dependency Extras

Install only the queue provider dependencies needed by the runtime environment:

```bash
pip install "etlplus[queue-aws]"
```

Provider extras:

- `etlplus[queue-aws]` for AWS SQS through `boto3`
- `etlplus[queue-azure]` for Azure Service Bus through `azure-servicebus`
- `etlplus[queue-gcp]` for Google Cloud Pub/Sub through `google-cloud-pubsub`
- `etlplus[queue-amqp]` for AMQP/RabbitMQ through `pika`
- `etlplus[queue-redis]` for Redis queue-like workflows through `redis`
- `etlplus[queue]` or `etlplus[queue-all]` for all queue provider dependencies

## Example

```python
from etlplus.connector import ConnectorQueue

connector = ConnectorQueue.from_obj(
    {
        'name': 'orders',
        'type': 'queue',
        'service': 'sqs',
        'queue_name': 'orders.fifo',
        'region': 'us-east-1',
        'options': {
            'visibility_timeout': 30,
            'message_group_id': 'orders',
        },
    },
)

queue_config = connector.to_queue_config()

assert queue_config.service.value == 'aws-sqs'
assert queue_config.queue_type.value == 'fifo'
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Queue connector metadata in [`etlplus.connector._queue`](../connector/_queue.py)
- Queue enums in [`_enums.py`](_enums.py)
- AWS SQS metadata in [`_sqs.py`](_sqs.py)
