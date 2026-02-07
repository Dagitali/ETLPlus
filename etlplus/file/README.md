# `etlplus.file` Subpackage

Documentation for the `etlplus.file` subpackage: unified file format support and helpers for reading
and writing data files.

- Provides a consistent interface for reading and writing files in various formats
- Defines many formats in `FileFormat`; read/write support varies by format
- Includes helpers for inferring file format and compression from filenames, extensions, or MIME
  types
- Exposes a `File` class with instance methods for reading and writing data

Some formats require optional dependencies. Install with:

```bash
pip install -e ".[file]"
```

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.file` Subpackage](#etlplusfile-subpackage)
  - [Handler Architecture](#handler-architecture)
  - [Supported File Formats](#supported-file-formats)
  - [Inferring File Format and Compression](#inferring-file-format-and-compression)
  - [Reading and Writing Files](#reading-and-writing-files)
    - [Reading a File](#reading-a-file)
    - [Writing a File](#writing-a-file)
  - [File Instance Methods](#file-instance-methods)
  - [Example: Reading and Writing](#example-reading-and-writing)
  - [See Also](#see-also)

## Supported File Formats

The following formats are implemented for reading/writing (unless noted). For the full support
matrix across all `FileFormat` values, see the top-level [README](../../README.md).

| Format    | Description                                 |
|-----------|---------------------------------------------|
| avro      | Apache Avro binary serialization            |
| arrow     | Apache Arrow IPC                            |
| bson      | Binary JSON (BSON)                          |
| cbor      | Concise Binary Object Representation        |
| csv       | Comma-separated values text files           |
| dat       | Generic data files (delimited)              |
| dta       | Stata datasets                              |
| duckdb    | DuckDB database file                        |
| feather   | Apache Arrow Feather columnar format        |
| fwf       | Fixed-width formatted text files            |
| gz        | Gzip-compressed files (see Compression)     |
| hdf5      | Hierarchical Data Format                    |
| ini       | INI config files                            |
| json      | Standard JSON files                         |
| msgpack   | MessagePack binary serialization            |
| nc        | NetCDF datasets                             |
| ndjson    | Newline-delimited JSON (JSON Lines)         |
| ods       | OpenDocument spreadsheets                   |
| orc       | Apache ORC columnar format                  |
| parquet   | Apache Parquet columnar format              |
| pb        | Protocol Buffers binary                     |
| properties | Java-style properties                     |
| proto     | Protocol Buffers schema                     |
| psv       | Pipe-separated values text files            |
| rda       | RData workspace bundles                     |
| rds       | RDS datasets                                |
| sas7bdat  | SAS datasets                                |
| sav       | SPSS datasets                               |
| sqlite    | SQLite database file                        |
| tab       | Tab-delimited text files                    |
| toml      | TOML config files                           |
| tsv       | Tab-separated values text files             |
| txt       | Plain text files                            |
| xls       | Microsoft Excel (legacy .xls; read-only)    |
| xlsm      | Microsoft Excel Macro-Enabled (XLSM)        |
| xlsx      | Microsoft Excel (modern .xlsx)              |
| xpt       | SAS transport files                         |
| zip       | ZIP-compressed files (see Compression)      |
| xml       | XML files                                   |
| yaml      | YAML files                                  |

Note: HDF5 support is read-only; writing is currently disabled.

Compression formats (gz, zip) are also supported as wrappers for other formats. Formats not listed
here are currently stubbed and will raise `NotImplementedError` on read/write.

## Handler Architecture

`etlplus.file` uses class-based handlers with abstract base classes in `etlplus/file/base.py`.
Category contracts include:

- Delimited text (`DelimitedTextFileHandlerABC`)
- Text/fixed-width (`TextFixedWidthFileHandlerABC`)
- Semi-structured text (`SemiStructuredTextFileHandlerABC`)
- Columnar (`ColumnarFileHandlerABC`)
- Binary serialization (`BinarySerializationFileHandlerABC`)
- Embedded databases (`EmbeddedDatabaseFileHandlerABC`)
- Spreadsheets (`SpreadsheetFileHandlerABC`)
- Read-only spreadsheets (`ReadOnlySpreadsheetFileHandlerABC`)
- Scientific/statistical datasets (`ScientificDatasetFileHandlerABC` and
  `SingleDatasetScientificFileHandlerABC`)
- Archive wrappers (`ArchiveWrapperFileHandlerABC`)
- Placeholder stubs (`StubFileHandlerABC`)

Format dispatch is registry-driven via explicit format-to-handler mappings.

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
- Supports the implemented formats listed above.

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
