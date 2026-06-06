# `etlplus.connector` Subpackage

Documentation for the `etlplus.connector` subpackage: connector configuration models and helpers
used by pipeline sources and targets.

- Defines connector metadata for API, database, file, and queue endpoints
- Normalizes connector configuration into typed dataclass objects
- Provides connector type enums and diagnostics policy helpers
- Keeps runtime clients separate from configuration metadata

Back to project overview: see the top-level [README](../../README.md).

- [Public API](#public-api)
- [Connector Types](#connector-types)
- [Extension Notes](#extension-notes)
- [See Also](#see-also)

## Public API

Most callers should use the package facade:

```python
from etlplus.connector import ConnectorApi, ConnectorDb, ConnectorFile, parse_connector
```

Public exports include:

- `ConnectorApi`, `ConnectorDb`, `ConnectorFile`, and `ConnectorQueue`: connector metadata classes.
- `DataConnectorType`: supported connector-type enum.
- `ConnectorDiagnosticPolicy`: shared diagnostic wording policy for connector checks.
- `parse_connector`: tolerant parser for connector configuration mappings.

## Connector Types

The stable connector metadata surface covers:

- `api`: HTTP endpoint metadata.
- `database`: database connection and provider metadata.
- `file`: local or remote file/storage metadata.
- `queue`: queue endpoint metadata.

## Extension Notes

Dynamic third-party plugin loading is not part of the current public runtime surface. The
underscore-prefixed plugin helper module is implementation groundwork and should not be treated as a
stable plugin API.

## See Also

- Queue metadata package in [`../queue/README.md`](../queue/README.md)
- Pipeline authoring guide in [docs/pipeline-guide.md](../../docs/pipeline-guide.md)
- Dependency and extension policy notes in
  [DEPENDENCY-AND-EXTENSION-POLICY-NOTES.md](../../DEPENDENCY-AND-EXTENSION-POLICY-NOTES.md)
