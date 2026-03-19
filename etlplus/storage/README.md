# `etlplus.storage` Subpackage

Documentation for the `etlplus.storage` subpackage: storage locations and backend helpers that sit
under the file-format layer.

- Separates where bytes live from how payloads are encoded
- Normalizes local paths and storage URIs into a shared `StorageLocation` model
- Resolves the active storage backend for a location
- Includes local-disk support plus SDK-backed S3, Azure Blob, and Azure Data Lake Storage Gen2
  (ABFS) access

Install the cloud storage dependencies with:

```bash
pip install -e ".[storage]"
```

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

The top-level `etlplus.file.File` wrapper now stages remote objects through
`etlplus.storage`, so URIs such as `s3://bucket/data.json`,
`azure-blob://container/data.json`, and
`abfs://filesystem@account.dfs.core.windows.net/data.json` can be used with
the existing file-format handlers.

## Public API

- `AbfsStorageBackend`: Azure Data Lake Storage Gen2 backend for
  `abfs://filesystem@account/path` locations.
- `AzureBlobStorageBackend`: Azure Blob backend for `azure-blob://container/blob` locations.
- `coerce_location(value)`: Normalize a mixed input into `StorageLocation`.
- `FtpStorageBackend`: Stub backend for `ftp://host/path` locations.
- `get_backend(value)`: Resolve the backend that can open or inspect the location.
- `LocalStorageBackend`: Local filesystem backend for `file` locations and `file://` URIs.
- `S3StorageBackend`: S3 backend for `s3://bucket/key` locations.
- `StorageLocation.from_value(value)`: Parse a local path or storage URI into a normalized
  location object.
- `StorageScheme`: Scheme enum used by parsed locations and backend resolution.
- `StubStorageBackend`: Shared placeholder base for unsupported remote backends.

## Example

```python
from etlplus.storage import StorageLocation
from etlplus.storage import get_backend

location = StorageLocation.from_value('data/input.csv')
backend = get_backend(location)

with backend.open(location, encoding='utf-8') as handle:
    payload = handle.read()
```

S3 uses the standard boto3 credential chain.

Azure Blob uses either `AZURE_STORAGE_CONNECTION_STRING` or
`AZURE_STORAGE_ACCOUNT_URL`. When using account URLs, you can also provide
`AZURE_STORAGE_CREDENTIAL`, or instantiate `AzureBlobStorageBackend` directly
with explicit connection settings.

ABFS uses `azure-storage-file-datalake`. It honors the same
`AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT_URL`, and
`AZURE_STORAGE_CREDENTIAL` environment variables, and can also derive the
account URL from the `abfs://filesystem@account-host/path` authority.

## Supported Schemes

- `abfs`: Azure Data Lake Storage Gen2 backend wired to `azure-storage-file-datalake`
- `azure-blob`: Azure Blob backend wired to `azure-storage-blob`
- `file`: Local filesystem paths and `file://` URIs
- `ftp`: FTP stub backend with validation-only runtime hooks
- `s3`: AWS S3 backend wired to `boto3`
