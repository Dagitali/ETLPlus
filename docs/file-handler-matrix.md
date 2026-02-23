# File Handler Matrix

This page is the file-handler guardrail for migration tracking and asymmetry checks.

Update this page after each `etlplus/file` migration batch.

- Source of truth: `etlplus/file/registry.py` explicit handler map.
- `status = stub` means the handler inherits `StubFileHandlerABC` directly or via internal category
  stubs in `etlplus/file/_stub_categories.py` (read/write raise `NotImplementedError`).
- `read/write support` reflects handler contract (`read/write` or `read-only`).
- Legacy module-level `etlplus.file.<format>.read()` / `write()` wrappers have been removed.
- Read/write calls should target handler instances directly (or `etlplus.file.core.File`).

| Format | Handler Class | Base ABC | Read/Write Support | Status |
| --- | --- | --- | --- | --- |
| `accdb` | `AccdbFile` | `StubEmbeddedDatabaseFileHandlerABC` | `read/write` | `stub` |
| `arrow` | `ArrowFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `avro` | `AvroFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `bson` | `BsonFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `cbor` | `CborFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `cfg` | `CfgFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `conf` | `ConfFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `csv` | `CsvFile` | `StandardDelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `dat` | `DatFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `dta` | `DtaFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `duckdb` | `DuckdbFile` | `EmbeddedDatabaseFileHandlerABC` | `read/write` | `implemented` |
| `feather` | `FeatherFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `fwf` | `FwfFile` | `TextFixedWidthFileHandlerABC` | `read/write` | `implemented` |
| `gz` | `GzFile` | `ArchiveWrapperFileHandlerABC` | `read/write` | `implemented` |
| `hbs` | `HbsFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `hdf5` | `Hdf5File` | `ScientificDatasetFileHandlerABC` | `read-only` | `implemented` |
| `ini` | `IniFile` | `DictPayloadSemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `ion` | `IonFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `jinja2` | `Jinja2File` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `json` | `JsonFile` | `RecordPayloadSemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `log` | `LogFile` | `LogEventFileHandlerABC` | `read/write` | `implemented` |
| `mat` | `MatFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |
| `mdb` | `MdbFile` | `StubEmbeddedDatabaseFileHandlerABC` | `read/write` | `stub` |
| `msgpack` | `MsgpackFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `mustache` | `MustacheFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `nc` | `NcFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `ndjson` | `NdjsonFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `numbers` | `NumbersFile` | `StubSpreadsheetFileHandlerABC` | `read/write` | `stub` |
| `ods` | `OdsFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `orc` | `OrcFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `parquet` | `ParquetFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `pb` | `PbFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `pbf` | `PbfFile` | `StubBinarySerializationFileHandlerABC` | `read/write` | `stub` |
| `properties` | `PropertiesFile` | `DictPayloadSemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `proto` | `ProtoFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `psv` | `PsvFile` | `StandardDelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `rda` | `RdaFile` | `ScientificDatasetFileHandlerABC` | `read/write` | `implemented` |
| `rds` | `RdsFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `sas7bdat` | `Sas7bdatFile` | `SingleDatasetScientificFileHandlerABC` | `read-only` | `implemented` |
| `sav` | `SavFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `sqlite` | `SqliteFile` | `EmbeddedDatabaseFileHandlerABC` | `read/write` | `implemented` |
| `stub` | `StubFile` | `StubFileHandlerABC` | `read/write` | `stub` |
| `sylk` | `SylkFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |
| `tab` | `TabFile` | `StandardDelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `toml` | `TomlFile` | `DictPayloadSemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `tsv` | `TsvFile` | `StandardDelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `txt` | `TxtFile` | `TextFixedWidthFileHandlerABC` | `read/write` | `implemented` |
| `vm` | `VmFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `wks` | `WksFile` | `StubSpreadsheetFileHandlerABC` | `read/write` | `stub` |
| `xls` | `XlsFile` | `ReadOnlySpreadsheetFileHandlerABC` | `read-only` | `implemented` |
| `xlsm` | `XlsmFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `xlsx` | `XlsxFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `xml` | `XmlFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `xpt` | `XptFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `yaml` | `YamlFile` | `RecordPayloadSemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `zip` | `ZipFile` | `ArchiveWrapperFileHandlerABC` | `read/write` | `implemented` |
| `zsav` | `ZsavFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |
