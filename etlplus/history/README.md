# `etlplus.history` Subpackage

Documentation for the `etlplus.history` subpackage: local-first run history storage and query
models.

- Stores normalized run records and job records for `etlplus run`
- Provides SQLite-backed persistence by default with JSONL fallback support
- Supports the `history`, `log`, `status`, `report`, and `ui` CLI surfaces
- Keeps the local history schema version explicit through `HISTORY_SCHEMA_VERSION`

Back to project overview: see the top-level [README](../../README.md).

- [Public API](#public-api)
- [Runtime Behavior](#runtime-behavior)
- [See Also](#see-also)

## Public API

Most callers should use the package facade:

```python
from etlplus.history import HistoryStore, RunRecord, SQLiteHistoryStore
```

Public exports include:

- `HistoryStore`: abstract history persistence contract.
- `SQLiteHistoryStore`: default local SQLite history store.
- `JsonlHistoryStore`: JSONL fallback history store.
- `RunRecord`, `RunCompletion`, and `RunState`: normalized history models.
- `HISTORY_SCHEMA_VERSION`: persisted history schema identifier.

## Runtime Behavior

`etlplus run` is the command that persists local run history. Other history commands read the same
store to list runs, inspect latest status, stream raw events, aggregate reports, or serve the local
read-only UI.

## See Also

- Run-history guide in [docs/run-history.md](../../docs/run-history.md)
- Published run-history guide in
  [docs/source/guides/run-history.md](../../docs/source/guides/run-history.md)
- Structured event guide in
  [docs/source/guides/structured-events.md](../../docs/source/guides/structured-events.md)
