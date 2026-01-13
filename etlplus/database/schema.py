"""
:mod:`etlplus.database.schema` module.

Helpers for loading and translating YAML definitions of database table schema
specifications into Pydantic models for dynamic SQLAlchemy generation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import ClassVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from ..file import File
from ..types import StrPath

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


# SECTION: CLASSES ========================================================== #


class ColumnSpec(BaseModel):
    """
    Column specification suitable for ODBC / SQLite DDL.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : str
        Unquoted column name.
    type : str
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

    name: str
    type: str = Field(description='SQL type string, e.g., INT, NVARCHAR(100)')
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
    columns : list[str]
        List of local column names.
    ref_table : str
        Referenced table name.
    ref_columns : list[str]
        List of referenced column names.
    ondelete : str | None
        ON DELETE action, or None.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    columns: list[str]
    ref_table: str
    ref_columns: list[str]
    ondelete: str | None = None


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
    name : str
        Index name.
    columns : list[str]
        List of column names included in the index.
    unique : bool
        True if the index is unique.
    where : str | None
        Optional WHERE clause for filtered indexes.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: str
    columns: list[str]
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
    columns : list[str]
        List of column names included in the primary key.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: str | None = None
    columns: list[str]


class UniqueConstraintSpec(BaseModel):
    """
    Unique constraint specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    name : str | None
        Unique constraint name, or None if unnamed.
    columns : list[str]
        List of column names included in the unique constraint.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    name: str | None = None
    columns: list[str]


class TableSpec(BaseModel):
    """
    Table specification.

    Attributes
    ----------
    model_config : ClassVar[ConfigDict]
        Pydantic model configuration.
    table : str
        Table name.
    schema_name : str | None
        Schema name, or None if not specified.
    create_schema : bool
        Whether to create the schema if it does not exist.
    columns : list[ColumnSpec]
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

    table: str = Field(alias='name')
    schema_name: str | None = Field(default=None, alias='schema')
    create_schema: bool = False
    columns: list[ColumnSpec]
    primary_key: PrimaryKeySpec | None = None
    unique_constraints: list[UniqueConstraintSpec] = Field(
        default_factory=list,
    )
    indexes: list[IndexSpec] = Field(default_factory=list)
    foreign_keys: list[ForeignKeySpec] = Field(default_factory=list)

    # -- Properties -- #

    @property
    def fq_name(self) -> str:
        """
        Fully qualified table name, including schema if specified.
        """
        return (
            f'{self.schema_name}.{self.table}'
            if self.schema_name
            else self.table
        )


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
    if not data:
        return []

    if isinstance(data, dict) and 'table_schemas' in data:
        items: list[Any] = data['table_schemas'] or []
    elif isinstance(data, list):
        items = data
    else:
        items = [data]

    return [TableSpec.model_validate(item) for item in items]
