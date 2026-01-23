# `etlplus.file` Subpackage

Documentation for the `etlplus.file` subpackage: unified file format support and helpers for reading
and writing data files.

- Provides a consistent interface for reading and writing files in various formats
- Supports all formats defined in `FileFormat` (see below)
- Includes helpers for inferring file format and compression from filenames, extensions, or MIME
  types
- Exposes a `File` class with instance methods for reading and writing data

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.file` Subpackage](#etlplusfile-subpackage)
  - [Supported File Formats](#supported-file-formats)
  - [Inferring File Format and Compression](#inferring-file-format-and-compression)
  - [Reading and Writing Files](#reading-and-writing-files)
    - [Reading a File](#reading-a-file)
    - [Writing a File](#writing-a-file)
  - [File Instance Methods](#file-instance-methods)
  - [Example: Reading and Writing](#example-reading-and-writing)
  - [See Also](#see-also)

## Supported File Formats

The following formats are defined in `FileFormat` and supported for reading and writing:

| Format    | Description                                 |
|-----------|---------------------------------------------|
| avro      | Apache Avro binary serialization            |
| csv       | Comma-separated values text files           |
| feather   | Apache Arrow Feather columnar format        |
| gz        | Gzip-compressed files (see Compression)     |
| json      | Standard JSON files                         |
| ndjson    | Newline-delimited JSON (JSON Lines)         |
| orc       | Apache ORC columnar format                  |
| parquet   | Apache Parquet columnar format              |
| tsv       | Tab-separated values text files             |
| txt       | Plain text files                            |
| xls       | Microsoft Excel (legacy .xls)               |
| xlsx      | Microsoft Excel (modern .xlsx)              |
| zip       | ZIP-compressed files (see Compression)      |
| xml       | XML files                                   |
| yaml      | YAML files                                  |

Compression formats (gz, zip) are also supported as wrappers for other formats.

## Inferring File Format and Compression

Use `infer_file_format_and_compression(value, filename=None)` to infer the file format and
compression from a filename, extension, or MIME type. Returns a tuple `(file_format,
compression_format)`.

## Reading and Writing Files

The main entry point for file operations is the `File` class. To read or write files:

### Reading a File

```python
from etlplus.file import File

f = File("data/sample.csv")
data = f.read()
```

- The `read()` method automatically detects the format and compression.
- Returns parsed data (e.g., list of dicts for tabular formats).

### Writing a File

```python
from etlplus.file import File

f = File("output.json")
f.write(data)
```

- The `write()` method serializes and writes data in the appropriate format.
- Supports all formats listed above.

## File Instance Methods

- `read()`: Reads and parses the file, returning structured data.
- `write(data)`: Writes structured data to the file in the detected format.

## Example: Reading and Writing

```python
from etlplus.file import File

# Read CSV
csv_file = File("data.csv")
rows = csv_file.read()

# Write JSON
json_file = File("output.json")
json_file.write(rows)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- File format enums in [enums.py](enums.py)
- Compression format enums in [enums.py](enums.py)
