"""
:mod:`etlplus.file.registry` module.

Class-based file handler registry.
"""

from __future__ import annotations

import importlib
from functools import cache
from typing import cast

from .base import FileHandlerABC
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'get_handler',
    'get_handler_class',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


# Explicit class map is the default dispatch path.
_HANDLER_CLASS_SPECS: dict[FileFormat, str] = {
    FileFormat.ACCDB: 'etlplus.file.accdb:AccdbFile',
    FileFormat.AVRO: 'etlplus.file.avro:AvroFile',
    FileFormat.ARROW: 'etlplus.file.arrow:ArrowFile',
    FileFormat.BSON: 'etlplus.file.bson:BsonFile',
    FileFormat.CBOR: 'etlplus.file.cbor:CborFile',
    FileFormat.CFG: 'etlplus.file.cfg:CfgFile',
    FileFormat.CONF: 'etlplus.file.conf:ConfFile',
    FileFormat.CSV: 'etlplus.file.csv:CsvFile',
    FileFormat.DAT: 'etlplus.file.dat:DatFile',
    FileFormat.DTA: 'etlplus.file.dta:DtaFile',
    FileFormat.DUCKDB: 'etlplus.file.duckdb:DuckdbFile',
    FileFormat.FEATHER: 'etlplus.file.feather:FeatherFile',
    FileFormat.FWF: 'etlplus.file.fwf:FwfFile',
    FileFormat.GZ: 'etlplus.file.gz:GzFile',
    FileFormat.HDF5: 'etlplus.file.hdf5:Hdf5File',
    FileFormat.HBS: 'etlplus.file.hbs:HbsFile',
    FileFormat.INI: 'etlplus.file.ini:IniFile',
    FileFormat.ION: 'etlplus.file.ion:IonFile',
    FileFormat.JINJA2: 'etlplus.file.jinja2:Jinja2File',
    FileFormat.JSON: 'etlplus.file.json:JsonFile',
    FileFormat.LOG: 'etlplus.file.log:LogFile',
    FileFormat.MSGPACK: 'etlplus.file.msgpack:MsgpackFile',
    FileFormat.MAT: 'etlplus.file.mat:MatFile',
    FileFormat.MDB: 'etlplus.file.mdb:MdbFile',
    FileFormat.MUSTACHE: 'etlplus.file.mustache:MustacheFile',
    FileFormat.NC: 'etlplus.file.nc:NcFile',
    FileFormat.NDJSON: 'etlplus.file.ndjson:NdjsonFile',
    FileFormat.NUMBERS: 'etlplus.file.numbers:NumbersFile',
    FileFormat.ODS: 'etlplus.file.ods:OdsFile',
    FileFormat.ORC: 'etlplus.file.orc:OrcFile',
    FileFormat.PARQUET: 'etlplus.file.parquet:ParquetFile',
    FileFormat.PB: 'etlplus.file.pb:PbFile',
    FileFormat.PBF: 'etlplus.file.pbf:PbfFile',
    FileFormat.PROTO: 'etlplus.file.proto:ProtoFile',
    FileFormat.PROPERTIES: 'etlplus.file.properties:PropertiesFile',
    FileFormat.PSV: 'etlplus.file.psv:PsvFile',
    FileFormat.RDA: 'etlplus.file.rda:RdaFile',
    FileFormat.RDS: 'etlplus.file.rds:RdsFile',
    FileFormat.SAS7BDAT: 'etlplus.file.sas7bdat:Sas7bdatFile',
    FileFormat.SAV: 'etlplus.file.sav:SavFile',
    FileFormat.SQLITE: 'etlplus.file.sqlite:SqliteFile',
    FileFormat.SYLK: 'etlplus.file.sylk:SylkFile',
    FileFormat.TAB: 'etlplus.file.tab:TabFile',
    FileFormat.TOML: 'etlplus.file.toml:TomlFile',
    FileFormat.TXT: 'etlplus.file.txt:TxtFile',
    FileFormat.TSV: 'etlplus.file.tsv:TsvFile',
    FileFormat.STUB: 'etlplus.file.stub:StubFile',
    FileFormat.VM: 'etlplus.file.vm:VmFile',
    FileFormat.WKS: 'etlplus.file.wks:WksFile',
    FileFormat.XML: 'etlplus.file.xml:XmlFile',
    FileFormat.XLS: 'etlplus.file.xls:XlsFile',
    FileFormat.XLSM: 'etlplus.file.xlsm:XlsmFile',
    FileFormat.XLSX: 'etlplus.file.xlsx:XlsxFile',
    FileFormat.XPT: 'etlplus.file.xpt:XptFile',
    FileFormat.YAML: 'etlplus.file.yaml:YamlFile',
    FileFormat.ZSAV: 'etlplus.file.zsav:ZsavFile',
    FileFormat.ZIP: 'etlplus.file.zip:ZipFile',
}


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_handler_class(
    symbol: object,
    *,
    file_format: FileFormat,
) -> type[FileHandlerABC]:
    """
    Validate and coerce *symbol* into a handler class.

    Parameters
    ----------
    symbol : object
        Imported object to validate.
    file_format : FileFormat
        File format associated with the imported handler.

    Returns
    -------
    type[FileHandlerABC]
        Concrete handler class.

    Raises
    ------
    ValueError
        If the imported symbol is not a handler class.
    """
    if not isinstance(symbol, type):
        raise ValueError(
            f'Handler for {file_format.value!r} must be a class, got '
            f'{type(symbol).__name__}',
        )
    if not issubclass(symbol, FileHandlerABC):
        raise ValueError(
            f'Handler for {file_format.value!r} must inherit FileHandlerABC',
        )
    return cast(type[FileHandlerABC], symbol)


def _import_symbol(
    spec: str,
) -> object:
    """
    Import and return a symbol in ``module:attribute`` format.

    Parameters
    ----------
    spec : str
        Import specification in ``module:attribute`` form.

    Returns
    -------
    object
        Imported symbol.

    Raises
    ------
    ValueError
        If *spec* is malformed or symbol import fails.
    """
    parts = spec.split(':', maxsplit=1)
    if len(parts) != 2:
        raise ValueError(f'Invalid handler spec: {spec!r}')
    module_name, attr = parts
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as err:
        raise ValueError(
            f'Handler symbol {attr!r} not found in module {module_name!r}',
        ) from err


# SECTION: FUNCTIONS ======================================================== #


@cache
def get_handler_class(
    file_format: FileFormat,
) -> type[FileHandlerABC]:
    """
    Resolve a handler class for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.
    Returns
    -------
    type[FileHandlerABC]
        Concrete handler class for the format.

    Raises
    ------
    ValueError
        If the format has no registered handler.
    """
    spec = _HANDLER_CLASS_SPECS.get(file_format)
    if spec is not None:
        try:
            symbol = _import_symbol(spec)
        except (ModuleNotFoundError, ValueError) as err:
            raise ValueError(f'Unsupported format: {file_format}') from err
        return _coerce_handler_class(symbol, file_format=file_format)

    raise ValueError(f'Unsupported format: {file_format}')


@cache
def get_handler(
    file_format: FileFormat,
) -> FileHandlerABC:
    """
    Return a singleton handler instance for *file_format*.

    Parameters
    ----------
    file_format : FileFormat
        File format enum value.
    Returns
    -------
    FileHandlerABC
        Singleton handler instance.
    """
    return get_handler_class(file_format)()
