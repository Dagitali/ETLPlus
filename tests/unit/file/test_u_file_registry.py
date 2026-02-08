"""
:mod:`tests.unit.file.test_u_file_registry` module.

Unit tests for :mod:`etlplus.file.registry`.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import etlplus.file as file_package
from etlplus.file import FileFormat
from etlplus.file import registry as mod
from etlplus.file.arrow import ArrowFile
from etlplus.file.avro import AvroFile
from etlplus.file.base import ArchiveWrapperFileHandlerABC
from etlplus.file.base import BinarySerializationFileHandlerABC
from etlplus.file.base import ColumnarFileHandlerABC
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import EmbeddedDatabaseFileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOnlySpreadsheetFileHandlerABC
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SemiStructuredTextFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import SpreadsheetFileHandlerABC
from etlplus.file.base import TextFixedWidthFileHandlerABC
from etlplus.file.dta import DtaFile
from etlplus.file.duckdb import DuckdbFile
from etlplus.file.feather import FeatherFile
from etlplus.file.fwf import FwfFile
from etlplus.file.json import JsonFile
from etlplus.file.mat import MatFile
from etlplus.file.nc import NcFile
from etlplus.file.ods import OdsFile
from etlplus.file.orc import OrcFile
from etlplus.file.parquet import ParquetFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sas7bdat import Sas7bdatFile
from etlplus.file.sav import SavFile
from etlplus.file.sqlite import SqliteFile
from etlplus.file.stub import StubFileHandlerABC
from etlplus.file.sylk import SylkFile
from etlplus.file.txt import TxtFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zsav import ZsavFile
from tests.unit.file.conftest import RegistryAbcConformanceContract
from tests.unit.file.conftest import RegistryFallbackPolicyContract
from tests.unit.file.conftest import RegistryMappedResolutionContract

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(autouse=True)
def clear_registry_caches() -> Iterator[None]:
    """
    Clear registry caches before and after each test.
    """
    # pylint: disable=protected-access

    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()
    mod._module_adapter_class_for_format.cache_clear()
    mod._module_for_format.cache_clear()
    yield
    mod.get_handler.cache_clear()
    mod.get_handler_class.cache_clear()
    mod._module_adapter_class_for_format.cache_clear()
    mod._module_for_format.cache_clear()


# SECTION: TESTS ============================================================ #


class TestRegistryAbcConformance(RegistryAbcConformanceContract):
    """Unit tests for category-to-ABC inheritance conformance."""

    registry_module = mod
    abc_cases = [
        # Delimited text
        (FileFormat.CSV, DelimitedTextFileHandlerABC),
        (FileFormat.DAT, DelimitedTextFileHandlerABC),
        (FileFormat.PSV, DelimitedTextFileHandlerABC),
        (FileFormat.TAB, DelimitedTextFileHandlerABC),
        (FileFormat.TSV, DelimitedTextFileHandlerABC),
        # Semi-structured
        (FileFormat.INI, SemiStructuredTextFileHandlerABC),
        (FileFormat.JSON, SemiStructuredTextFileHandlerABC),
        (FileFormat.NDJSON, SemiStructuredTextFileHandlerABC),
        (FileFormat.PROPERTIES, SemiStructuredTextFileHandlerABC),
        (FileFormat.TOML, SemiStructuredTextFileHandlerABC),
        (FileFormat.XML, SemiStructuredTextFileHandlerABC),
        (FileFormat.YAML, SemiStructuredTextFileHandlerABC),
        # Columnar
        (FileFormat.ARROW, ColumnarFileHandlerABC),
        (FileFormat.FEATHER, ColumnarFileHandlerABC),
        (FileFormat.ORC, ColumnarFileHandlerABC),
        (FileFormat.PARQUET, ColumnarFileHandlerABC),
        # Binary serialization/interchange
        (FileFormat.AVRO, BinarySerializationFileHandlerABC),
        (FileFormat.BSON, BinarySerializationFileHandlerABC),
        (FileFormat.CBOR, BinarySerializationFileHandlerABC),
        (FileFormat.MSGPACK, BinarySerializationFileHandlerABC),
        (FileFormat.PB, BinarySerializationFileHandlerABC),
        (FileFormat.PROTO, BinarySerializationFileHandlerABC),
        # Embedded databases
        (FileFormat.DUCKDB, EmbeddedDatabaseFileHandlerABC),
        (FileFormat.SQLITE, EmbeddedDatabaseFileHandlerABC),
        # Spreadsheets
        (FileFormat.ODS, SpreadsheetFileHandlerABC),
        (FileFormat.XLS, SpreadsheetFileHandlerABC),
        (FileFormat.XLSM, SpreadsheetFileHandlerABC),
        (FileFormat.XLSX, SpreadsheetFileHandlerABC),
        # Plain text/fixed-width text
        (FileFormat.FWF, TextFixedWidthFileHandlerABC),
        (FileFormat.TXT, TextFixedWidthFileHandlerABC),
        # Archives
        (FileFormat.GZ, ArchiveWrapperFileHandlerABC),
        (FileFormat.ZIP, ArchiveWrapperFileHandlerABC),
        # Scientific/statistical
        (FileFormat.DTA, ScientificDatasetFileHandlerABC),
        (FileFormat.HDF5, ScientificDatasetFileHandlerABC),
        (FileFormat.MAT, ScientificDatasetFileHandlerABC),
        (FileFormat.NC, ScientificDatasetFileHandlerABC),
        (FileFormat.RDA, ScientificDatasetFileHandlerABC),
        (FileFormat.RDS, ScientificDatasetFileHandlerABC),
        (FileFormat.SAS7BDAT, ScientificDatasetFileHandlerABC),
        (FileFormat.SAV, ScientificDatasetFileHandlerABC),
        (FileFormat.SYLK, ScientificDatasetFileHandlerABC),
        (FileFormat.XPT, ScientificDatasetFileHandlerABC),
        (FileFormat.ZSAV, ScientificDatasetFileHandlerABC),
        # Single-dataset scientific subtype
        (FileFormat.DTA, SingleDatasetScientificFileHandlerABC),
        (FileFormat.MAT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.NC, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SAS7BDAT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SAV, SingleDatasetScientificFileHandlerABC),
        (FileFormat.SYLK, SingleDatasetScientificFileHandlerABC),
        (FileFormat.XPT, SingleDatasetScientificFileHandlerABC),
        (FileFormat.ZSAV, SingleDatasetScientificFileHandlerABC),
        # Placeholder/stubbed module-owned formats
        (FileFormat.STUB, StubFileHandlerABC),
        (FileFormat.ACCDB, StubFileHandlerABC),
        (FileFormat.CFG, StubFileHandlerABC),
        (FileFormat.CONF, StubFileHandlerABC),
        (FileFormat.HBS, StubFileHandlerABC),
        (FileFormat.ION, StubFileHandlerABC),
        (FileFormat.JINJA2, StubFileHandlerABC),
        (FileFormat.LOG, StubFileHandlerABC),
        (FileFormat.MDB, StubFileHandlerABC),
        (FileFormat.MUSTACHE, StubFileHandlerABC),
        (FileFormat.NUMBERS, StubFileHandlerABC),
        (FileFormat.PBF, StubFileHandlerABC),
        (FileFormat.VM, StubFileHandlerABC),
        (FileFormat.WKS, StubFileHandlerABC),
        # Read-only
        (FileFormat.HDF5, ReadOnlyFileHandlerABC),
        (FileFormat.SAS7BDAT, ReadOnlyFileHandlerABC),
        (FileFormat.XLS, ReadOnlyFileHandlerABC),
        # Read-only spreadsheets
        (FileFormat.XLS, ReadOnlySpreadsheetFileHandlerABC),
    ]


class TestRegistryMappedResolution(RegistryMappedResolutionContract):
    """Unit tests for explicitly mapped handler class resolution."""

    registry_module = mod
    file_package = file_package
    singleton_class = JsonFile
    mapped_class_cases = [
        (FileFormat.AVRO, AvroFile),
        (FileFormat.ARROW, ArrowFile),
        (FileFormat.DTA, DtaFile),
        (FileFormat.DUCKDB, DuckdbFile),
        (FileFormat.FEATHER, FeatherFile),
        (FileFormat.FWF, FwfFile),
        (FileFormat.JSON, JsonFile),
        (FileFormat.MAT, MatFile),
        (FileFormat.NC, NcFile),
        (FileFormat.ODS, OdsFile),
        (FileFormat.ORC, OrcFile),
        (FileFormat.PARQUET, ParquetFile),
        (FileFormat.RDA, RdaFile),
        (FileFormat.RDS, RdsFile),
        (FileFormat.SAS7BDAT, Sas7bdatFile),
        (FileFormat.SAV, SavFile),
        (FileFormat.SQLITE, SqliteFile),
        (FileFormat.SYLK, SylkFile),
        (FileFormat.TXT, TxtFile),
        (FileFormat.XLSM, XlsmFile),
        (FileFormat.XLSX, XlsxFile),
        (FileFormat.XPT, XptFile),
        (FileFormat.ZSAV, ZsavFile),
    ]
    placeholder_spec_cases = [
        (FileFormat.ACCDB, 'etlplus.file.accdb:AccdbFile'),
        (FileFormat.CFG, 'etlplus.file.cfg:CfgFile'),
        (FileFormat.CONF, 'etlplus.file.conf:ConfFile'),
        (FileFormat.HBS, 'etlplus.file.hbs:HbsFile'),
        (FileFormat.ION, 'etlplus.file.ion:IonFile'),
        (FileFormat.JINJA2, 'etlplus.file.jinja2:Jinja2File'),
        (FileFormat.LOG, 'etlplus.file.log:LogFile'),
        (FileFormat.MDB, 'etlplus.file.mdb:MdbFile'),
        (FileFormat.MUSTACHE, 'etlplus.file.mustache:MustacheFile'),
        (FileFormat.NUMBERS, 'etlplus.file.numbers:NumbersFile'),
        (FileFormat.PBF, 'etlplus.file.pbf:PbfFile'),
        (FileFormat.VM, 'etlplus.file.vm:VmFile'),
        (FileFormat.WKS, 'etlplus.file.wks:WksFile'),
    ]


class TestRegistryFallbackPolicy(RegistryFallbackPolicyContract):
    """Unit tests for registry fallback/deprecation and strict policies."""

    registry_module = mod
