# Integration File Smoke Conventions

`tests/integration/file/test_i_file_smoke.py` uses a single parameterized smoke
matrix for file-format read/write coverage. Cases live in
`tests/integration/file/pytest_smoke_file_contracts.py` as `FILE_SMOKE_CASES`.

- [Default Pattern](#default-pattern)
- [Documented Exceptions](#documented-exceptions)
- [Notes](#notes)

## Default Pattern

For most file formats, add one `FileSmokeCase` using the file-format module name:

```python
FileSmokeCase("csv")
```

The default smoke path is derived from the handler format:

- `data.<format>`

Examples:

- `csv` -> `data.csv`
- `parquet` -> `data.parquet`
- `sqlite` -> `data.sqlite`

## Documented Exceptions

Only the following cases should override the default path/error behavior:

| Case | Override | Reason |
| --- | --- | --- |
| `gz` | `file_name = "data.json.gz"` | `.gz` requires an inner format extension so the wrapped payload format can be inferred. |
| `hdf5` | `expect_write_error = RuntimeError`, `error_match = "read-only"` | `hdf5` handler is intentionally read-only. |
| `sas7bdat` | `expect_write_error = RuntimeError`, `error_match = "read-only"` | `sas7bdat` handler is intentionally read-only. |
| `zip` | `file_name = "data.json.zip"` | `.zip` requires an inner format extension so the wrapped payload format can be inferred. |
| `xls` | `expect_write_error = RuntimeError`, `error_match = "read-only"` | `xls` handler is intentionally read-only. |

## Notes

- Format-specific payload overrides, for example `proto`, `pb`, `xml`, and
  `ini`, are acceptable and are not part of path symmetry.
- New integration file smoke coverage should use the default case shape unless the format falls into
  one of the documented exceptions above.
