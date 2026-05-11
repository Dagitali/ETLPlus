# etlplus.queue

Queue type helpers for message-oriented ETL sources and targets.

The initial public surface models AWS SQS queues, including standard and FIFO queue types. Runtime
queue clients are intentionally outside this package for now; queue-backed pipeline endpoints are
represented through `etlplus.connector.ConnectorQueue`.
