"""
:mod:`etlplus.database.orm` module.

Dynamic SQLAlchemy model generation from YAML table specs.

Usage
-----
>>> from etlplus.database.orm import load_and_build_models
>>> registry = load_and_build_models('examples/configs/ddl_spec.yml')
>>> Player = registry['dbo.Customers']
"""

from __future__ import annotations

import re
from typing import Any
from typing import Final

from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import Numeric
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import Time
from sqlalchemy import UniqueConstraint
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import TypeEngine

from ..types import StrPath
from .schema import ForeignKeySpec
from .schema import TableSpec
from .schema import load_table_specs
from .types import ModelRegistry
from .types import TypeFactory

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'Base',
    # Functions
    'build_models',
    'load_and_build_models',
    'resolve_type',
]


# SECTION: INTERNAL CONSTANTS =============================================== #

_TYPE_MAPPING: Final[dict[str, TypeFactory]] = {
    'int': lambda _: Integer(),
    'integer': lambda _: Integer(),
    'bigint': lambda _: Integer(),
    'smallint': lambda _: Integer(),
    'bool': lambda _: Boolean(),
    'boolean': lambda _: Boolean(),
    'uuid': lambda _: PG_UUID(as_uuid=True),
    'uniqueidentifier': lambda _: PG_UUID(as_uuid=True),
    'rowversion': lambda _: LargeBinary(),
    'varbinary': lambda _: LargeBinary(),
    'blob': lambda _: LargeBinary(),
    'text': lambda _: Text(),
    'string': lambda _: Text(),
    'varchar': lambda p: String(length=p[0]) if p else String(),
    'nvarchar': lambda p: String(length=p[0]) if p else String(),
    'char': lambda p: String(length=p[0] if p else 1),
    'nchar': lambda p: String(length=p[0] if p else 1),
    'numeric': lambda p: Numeric(
        precision=p[0] if p else None,
        scale=p[1] if len(p) > 1 else None,
    ),
    'decimal': lambda p: Numeric(
        precision=p[0] if p else None,
        scale=p[1] if len(p) > 1 else None,
    ),
    'float': lambda _: Float(),
    'real': lambda _: Float(),
    'double': lambda _: Float(),
    'datetime': lambda _: DateTime(timezone=True),
    'datetime2': lambda _: DateTime(timezone=True),
    'timestamp': lambda _: DateTime(timezone=True),
    'date': lambda _: Date(),
    'time': lambda _: Time(),
    'json': lambda _: JSONB(),
    'jsonb': lambda _: JSONB(),
}


# SECTION: CLASSES ========================================================== #


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    __abstract__ = True


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _class_name(
    table: str,
) -> str:
    """
    Convert table name to PascalCase class name.

    Parameters
    ----------
    table : str
        Table name.

    Returns
    -------
    str
        PascalCase class name.
    """
    parts = re.split(r'[^A-Za-z0-9]+', table)
    return ''.join(p.capitalize() for p in parts if p)


def _parse_type_decl(
    type_str: str,
) -> tuple[str, list[int]]:
    """
    Parse a type declaration string into its name and parameters.

    Parameters
    ----------
    type_str : str
        Type declaration string, e.g., "varchar(255)".

    Returns
    -------
    tuple[str, list[int]]
        A tuple containing the type name and a list of integer parameters.
    """
    m = re.match(
        r'^(?P<name>[A-Za-z0-9_]+)(?:\((?P<params>[^)]*)\))?$',
        type_str.strip(),
    )
    if not m:
        return type_str.lower(), []
    name = m.group('name').lower()
    params_raw = m.group('params')
    if not params_raw:
        return name, []
    params = [p.strip() for p in params_raw.split(',') if p.strip()]
    parsed: list[int] = []
    for p in params:
        try:
            parsed.append(int(p))
        except ValueError:
            continue
    return name, parsed


def _table_kwargs(
    spec: TableSpec,
) -> dict[str, str]:
    """
    Generate table keyword arguments based on the table specification.

    Parameters
    ----------
    spec : TableSpec
        Table specification.

    Returns
    -------
    dict[str, str]
        Dictionary of table keyword arguments.
    """
    kwargs: dict[str, str] = {}
    if spec.schema_name:
        kwargs['schema'] = spec.schema_name
    return kwargs


# SECTION: FUNCTIONS ======================================================== #


def build_models(
    specs: list[TableSpec],
    *,
    base: type[DeclarativeBase] = Base,
) -> ModelRegistry:
    """
    Build SQLAlchemy ORM models from table specifications.

    Parameters
    ----------
    specs : list[TableSpec]
        List of table specifications.
    base : type[DeclarativeBase], optional
        Base class for the ORM models (default: :class:`Base`).

    Returns
    -------
    ModelRegistry
        Registry mapping fully qualified table names to ORM model classes.
    """
    registry: ModelRegistry = {}

    for spec in specs:
        table_args: list[object] = []
        table_kwargs = _table_kwargs(spec)
        pk_cols = set(spec.primary_key.columns) if spec.primary_key else set()

        # Pre-handle multi-column constraints.
        if spec.primary_key and len(spec.primary_key.columns) > 1:
            table_args.append(
                PrimaryKeyConstraint(
                    *spec.primary_key.columns,
                    name=spec.primary_key.name,
                ),
            )
        for uc in spec.unique_constraints:
            table_args.append(UniqueConstraint(*uc.columns, name=uc.name))
        for idx in spec.indexes:
            table_args.append(
                Index(
                    idx.name,
                    *idx.columns,
                    unique=idx.unique,
                    postgresql_where=text(idx.where) if idx.where else None,
                ),
            )
        composite_fks = [fk for fk in spec.foreign_keys if len(fk.columns) > 1]
        for fk in composite_fks:
            table_args.append(
                ForeignKeyConstraint(
                    fk.columns,
                    [f'{fk.ref_table}.{c}' for c in fk.ref_columns],
                    ondelete=fk.ondelete,
                ),
            )

        fk_by_column = {
            fk.columns[0]: fk
            for fk in spec.foreign_keys
            if len(fk.columns) == 1 and len(fk.ref_columns) == 1
        }

        attrs: dict[str, object] = {'__tablename__': spec.table}

        for col in spec.columns:
            col_fk: ForeignKeySpec | None = fk_by_column.get(col.name)
            fk_arg = (
                ForeignKey(
                    f'{col_fk.ref_table}.{col_fk.ref_columns[0]}',
                    ondelete=col_fk.ondelete,
                )
                if col_fk
                else None
            )
            col_type: TypeEngine = (
                Enum(*col.enum, name=f'{spec.table}_{col.name}_enum')
                if col.enum
                else resolve_type(col.type)
            )
            fk_args: list[ForeignKey] = []
            if fk_arg:
                fk_args.append(fk_arg)

            kwargs: dict[str, Any] = {
                'nullable': col.nullable,
                'primary_key': col.name in pk_cols and len(pk_cols) == 1,
                'unique': col.unique,
            }
            if col.default:
                kwargs['server_default'] = text(col.default)
            if col.identity:
                kwargs['autoincrement'] = True

            attrs[col.name] = mapped_column(*fk_args, type_=col_type, **kwargs)

            if col.check:
                table_args.append(
                    CheckConstraint(
                        col.check,
                        name=f'ck_{spec.table}_{col.name}',
                    ),
                )

        if table_args or table_kwargs:
            args_tuple = tuple(table_args)
            attrs['__table_args__'] = (
                (*args_tuple, table_kwargs) if table_kwargs else args_tuple
            )

        cls_name = _class_name(spec.table)
        model_cls = type(cls_name, (base,), attrs)
        registry[spec.fq_name] = model_cls

    return registry


def load_and_build_models(
    path: StrPath,
    *,
    base: type[DeclarativeBase] = Base,
) -> ModelRegistry:
    """
    Load table specifications from a file and build SQLAlchemy models.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file containing table specifications.
    base : type[DeclarativeBase], optional
        Base class for the ORM models (default: :class:`Base`).

    Returns
    -------
    ModelRegistry
        Registry mapping fully qualified table names to ORM model classes.
    """
    return build_models(load_table_specs(path), base=base)


def resolve_type(
    type_str: str,
) -> TypeEngine:
    """
    Resolve a string type declaration to a SQLAlchemy :class:`TypeEngine`.

    Parameters
    ----------
    type_str : str
        String representation of the type declaration.

    Returns
    -------
    TypeEngine
        SQLAlchemy type engine instance corresponding to the type declaration.
    """
    name, params = _parse_type_decl(type_str)
    factory = _TYPE_MAPPING.get(name)
    if factory:
        return factory(params)
    return Text()
