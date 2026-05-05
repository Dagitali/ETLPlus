"""
:mod:`etlplus.database._schema` module.

Helpers for loading and translating YAML definitions of database table schema
specifications into Pydantic models for dynamic SQLAlchemy generation.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from ..file import File
from ..utils import MappingParser
from ..utils._types import StrPath
from ._enums import ReferentialAction

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ColumnSpec',
    'ForeignKeySpec',
    'IdentitySpec',
    'IndexSpec',
    'PrimaryKeySpec',
    'UniqueConstraintSpec',
    'TableSpec',
    # Functions
    'load_table_specs',
]


# SECTION: TYPE ALIASES ===================================================== #


type NonEmptyStr = Annotated[str, Field(min_length=1)]
type NonEmptyStrList = Annotated[list[NonEmptyStr], Field(min_length=1)]


# SECTION: CLASSES ========================================================== #


class ColumnSpec(BaseModel):
    """
    Column specification suitable for ODBC / SQLite DDL.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : NonEmptyStr
        Unquoted column name.
    type : NonEmptyStr
        SQL type string, e.g., INT, NVARCHAR(100).
    nullable : bool
        True if NULL values are allowed.
    default : str | None
        Default value expression, or None if no default.
    identity : IdentitySpec | None
        Identity specification, or None if not an identity column.
    check : str | None
        Check constraint expression, or None if no check constraint.
    enum : list[str] | None
        List of allowed string values for enum-like columns, or None.
    unique : bool
        True if the column has a UNIQUE constraint.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: NonEmptyStr
    type: NonEmptyStr = Field(description='SQL type string, e.g., INT, NVARCHAR(100)')
    nullable: bool = True
    default: str | None = None
    identity: IdentitySpec | None = None
    check: str | None = None
    enum: list[str] | None = None
    unique: bool = False


class ForeignKeySpec(BaseModel):
    """
    Foreign key specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    columns : NonEmptyStrList
        List of local column names.
    ref_table : NonEmptyStr
        Referenced table name.
    ref_columns : NonEmptyStrList
        List of referenced column names.
    ondelete : str | None
        ON DELETE action, or None.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    columns: NonEmptyStrList
    ref_table: NonEmptyStr
    ref_columns: NonEmptyStrList
    ondelete: str | None = None

    # -- Validators -- #

    @field_validator('ondelete', mode='before')
    @classmethod
    def _normalize_ondelete(cls, value: object) -> str | None:
        """Normalize foreign key referential actions to SQL spelling."""
        if value is None:
            return None
        return ReferentialAction.coerce(value).sql

    @model_validator(mode='after')
    def _validate_column_counts(self) -> Self:
        """Validate local and referenced foreign-key column arity."""
        if len(self.columns) != len(self.ref_columns):
            raise ValueError('foreign key columns and ref_columns must match')
        return self


class IdentitySpec(BaseModel):
    """
    Identity specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    seed : int | None
        Identity seed value (default: 1).
    increment : int | None
        Identity increment value (default: 1).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    seed: int | None = Field(default=None, ge=1)
    increment: int | None = Field(default=None, ge=1)


class IndexSpec(BaseModel):
    """
    Index specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : NonEmptyStr
        Index name.
    columns : NonEmptyStrList
        List of column names included in the index.
    unique : bool
        True if the index is unique.
    where : str | None
        Optional WHERE clause for filtered indexes.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: NonEmptyStr
    columns: NonEmptyStrList
    unique: bool = False
    where: str | None = None


class PrimaryKeySpec(BaseModel):
    """
    Primary key specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : str | None
        Primary key constraint name, or None if unnamed.
    columns : NonEmptyStrList
        List of column names included in the primary key.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: str | None = None
    columns: NonEmptyStrList


class UniqueConstraintSpec(BaseModel):
    """
    Unique constraint specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : str | None
        Unique constraint name, or None if unnamed.
    columns : NonEmptyStrList
        List of column names included in the unique constraint.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: str | None = None
    columns: NonEmptyStrList


class TableSpec(BaseModel):
    """
    Table specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    table : NonEmptyStr
        Table name.
    schema_name : NonEmptyStr | None
        Schema name, or None if not specified.
    create_schema : bool
        Whether to create the schema if it does not exist.
    columns : Annotated[list[ColumnSpec], Field(min_length=1)]
        List of column specifications.
    primary_key : PrimaryKeySpec | None
        Primary key specification, or None if no primary key.
    unique_constraints : list[UniqueConstraintSpec]
        List of unique constraint specifications.
    indexes : list[IndexSpec]
        List of index specifications.
    foreign_keys : list[ForeignKeySpec]
        List of foreign key specifications.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    table: NonEmptyStr = Field(alias='name')
    schema_name: NonEmptyStr | None = Field(default=None, alias='schema')
    create_schema: bool = False
    columns: Annotated[list[ColumnSpec], Field(min_length=1)]
    primary_key: PrimaryKeySpec | None = None
    unique_constraints: list[UniqueConstraintSpec] = Field(
        default_factory=list,
    )
    indexes: list[IndexSpec] = Field(default_factory=list)
    foreign_keys: list[ForeignKeySpec] = Field(default_factory=list)

    # -- Getters -- #

    @property
    def fq_name(self) -> str:
        """Fully qualified table name, including schema if specified."""
        return f'{self.schema_name}.{self.table}' if self.schema_name else self.table


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _table_spec_items(data: Any) -> Sequence[Any]:
    """Normalize loaded table-spec payloads to a sequence of items."""
    if not data:
        return ()
    data_mapping = MappingParser.optional(data)
    if data_mapping is not None and 'table_schemas' in data_mapping:
        table_schemas = data_mapping['table_schemas']
        if table_schemas is None:
            return ()
        if not isinstance(table_schemas, list):
            raise TypeError('table_schemas must be a list')
        return table_schemas
    if isinstance(data, list):
        return data
    return (data,)


# SECTION: FUNCTIONS ======================================================== #


def load_table_specs(
    path: StrPath,
) -> list[TableSpec]:
    """
    Load table specifications from a YAML file.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file containing table specifications.

    Returns
    -------
    list[TableSpec]
        A list of TableSpec instances parsed from the YAML file.
    """
    data = File(Path(path)).read()
    return [TableSpec.model_validate(item) for item in _table_spec_items(data)]
