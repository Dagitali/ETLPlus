# etlplus.queue

Queue type helpers for message-oriented ETL sources and targets.

The initial public surface models queue configuration metadata for AWS SQS, Azure Service Bus,
Google Cloud Pub/Sub, AMQP/RabbitMQ, and Redis queue-like workflows. AWS SQS includes standard and
FIFO queue types plus common runtime metadata such as visibility timeout, long-poll wait time,
message retention, delay, dead-letter queue ARN, and FIFO message group/deduplication hints.

Runtime queue clients are intentionally outside this package for now; queue-backed pipeline
endpoints are represented through `etlplus.connector.ConnectorQueue`. Install `etlplus[queue-aws]`
to include the `boto3` dependency needed for SQS runtime integrations and readiness checks.

`QueueLocation` parses queue URI references such as `aws-sqs://us-east-1/events.fifo` and
`redis://localhost:6379/0/events`. `ConnectorQueue.to_queue_config()` converts generic queue
connectors into provider-specific queue config objects.

Provider-specific dependency extras are also defined for queue readiness and future runtime
integrations:

- `etlplus[queue-aws]` for AWS SQS through `boto3`
- `etlplus[queue-azure]` for Azure Service Bus through `azure-servicebus`
- `etlplus[queue-gcp]` for Google Cloud Pub/Sub through `google-cloud-pubsub`
- `etlplus[queue-amqp]` for AMQP/RabbitMQ through `pika`
- `etlplus[queue-redis]` for Redis queue-like workflows through `redis`
- `etlplus[queue]` or `etlplus[queue-all]` for all queue provider dependencies
