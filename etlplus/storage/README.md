# `etlplus.storage` Subpackage

Documentation for the `etlplus.storage` subpackage: storage locations and backend helpers that sit
under the file-format layer.

- Separates where bytes live from how payloads are encoded
- Normalizes local paths and storage URIs into a shared `StorageLocation` model
- Resolves the active storage backend for a location
- Starts with local-disk support while reserving scheme-aware entry points for future remote
  backends

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.storage` Subpackage](#etlplusstorage-subpackage)
  - [Relationship to `etlplus.file`](#relationship-to-etlplusfile)
  - [Public API](#public-api)
  - [Example](#example)
  - [Supported Schemes](#supported-schemes)

## Relationship to `etlplus.file`

`etlplus.file` remains the format layer for CSV, JSON, Parquet, and similar handlers.
`etlplus.storage` is the storage layer for local paths and future remote backends such as FTP,
S3-compatible object storage, and Azure storage services.

## Public API

- `AzureBlobStorageBackend`: Skeleton backend for `azure-blob://container/blob` locations.
- `coerce_location(value)`: Normalize a mixed input into `StorageLocation`.
- `get_backend(value)`: Resolve the backend that can open or inspect the location.
- `LocalStorageBackend`: Local filesystem backend for `file` locations and `file://` URIs.
- `StorageLocation.from_value(value)`: Parse a local path or storage URI into a normalized
  location object.
- `StorageScheme`: Scheme enum used by parsed locations and backend resolution.
- `S3StorageBackend`: Skeleton backend for `s3://bucket/key` locations.

## Example

```python
from etlplus.storage import StorageLocation
from etlplus.storage import get_backend

location = StorageLocation.from_value('data/input.csv')
backend = get_backend(location)

with backend.open(location, encoding='utf-8') as handle:
    payload = handle.read()
```

## Supported Schemes

- `abfs`: Reserved for future Azure Data Lake Storage Gen2 backend support
- `azure-blob`: Azure Blob skeleton backend with validation-only runtime hooks
- `file`: Local filesystem paths and `file://` URIs
- `ftp`: Reserved for future backend support
- `s3`: AWS S3 skeleton backend with validation-only runtime hooks
