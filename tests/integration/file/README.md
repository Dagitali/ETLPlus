# Integration File Smoke Conventions

`tests/integration/file/` uses `SmokeRoundtripModuleContract` as the default
pattern for file-format smoke tests.

- [Integration File Smoke Conventions](#integration-file-smoke-conventions)
  - [Default Pattern](#default-pattern)
  - [Documented Exceptions](#documented-exceptions)
  - [Notes](#notes)

## Default Pattern

For most file formats, keep the test class minimal:

```python
class TestCsv(SmokeRoundtripModuleContract):
    module = mod
```

The default smoke path is derived from the handler format:

- `data.<format>`

Examples:

- `csv` -> `data.csv`
- `parquet` -> `data.parquet`
- `sqlite` -> `data.sqlite`

## Documented Exceptions

Only the following modules should override the default path/error behavior:

| Module | Override | Reason |
| --- | --- | --- |
| `test_i_file_gz.py` | `file_name = "data.json.gz"` | `.gz` requires an inner format extension so the wrapped payload format can be inferred. |
| `test_i_file_zip.py` | `file_name = "data.json.zip"` | `.zip` requires an inner format extension so the wrapped payload format can be inferred. |
| `test_i_file_xls.py` | `expect_write_error = RuntimeError`, `error_match = "read-only"` | `xls` handler is intentionally read-only. |

## Notes

- Format-specific payload overrides (for example, `proto`, `pb`, `xml`, `ini`)
  are acceptable and are not part of path symmetry.
- New integration file smoke tests should use the default contract path unless
  they fall into one of the exceptions above.
