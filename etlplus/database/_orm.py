"""
:mod:`etlplus.database._orm` module.

Dynamic SQLAlchemy model generation from YAML table specs.

Usage
-----
>>> from etlplus.database import load_and_build_models
>>> registry = load_and_build_models('examples/configs/ddl_spec.yml')
>>> Player = registry['dbo.Customers']
"""

from __future__ import annotations

import re
from collections.abc import Sequence
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

from ..utils._types import StrPath
from ._schema import ColumnSpec
from ._schema import ForeignKeySpec
from ._schema import TableSpec
from ._schema import load_table_specs
from ._types import ModelRegistry
from ._types import TypeFactory

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

_TYPE_DECL_RE: Final[re.Pattern[str]] = re.compile(
    r'^(?P<name>[A-Za-z0-9_]+)(?:\((?P<params>[^)]*)\))?$',
)

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
) -> tuple[str, tuple[int, ...]]:
    """
    Parse a type declaration string into its name and parameters.

    Parameters
    ----------
    type_str : str
        Type declaration string, e.g., "varchar(255)".

    Returns
    -------
    tuple[str, tuple[int, ...]]
        A tuple containing the type name and a list of integer parameters.
    """
    match = _TYPE_DECL_RE.match(type_str.strip())
    if not match:
        return type_str.lower(), ()
    params_raw = match.group('params')
    return match.group('name').lower(), _parse_type_params(params_raw)


def _parse_type_params(params_raw: str | None) -> tuple[int, ...]:
    """Parse integer parameters from a SQL type declaration."""
    if not params_raw:
        return ()
    params = (p.strip() for p in params_raw.split(',') if p.strip())
    return tuple(int(p) for p in params if p.isdecimal())


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


def _append_table_constraints(
    table_args: list[object],
    spec: TableSpec,
) -> None:
    """Append table-level constraints and indexes for *spec*."""
    if spec.primary_key and len(spec.primary_key.columns) > 1:
        table_args.append(
            PrimaryKeyConstraint(
                *spec.primary_key.columns,
                name=spec.primary_key.name,
            ),
        )

    table_args.extend(
        UniqueConstraint(*uc.columns, name=uc.name)
        for uc in spec.unique_constraints
    )
    table_args.extend(
        Index(
            idx.name,
            *idx.columns,
            unique=idx.unique,
            postgresql_where=text(idx.where) if idx.where else None,
        )
        for idx in spec.indexes
    )
    table_args.extend(
        ForeignKeyConstraint(
            fk.columns,
            [f'{fk.ref_table}.{column}' for column in fk.ref_columns],
            ondelete=fk.ondelete,
        )
        for fk in spec.foreign_keys
        if len(fk.columns) > 1
    )


def _single_column_foreign_keys(
    spec: TableSpec,
) -> dict[str, ForeignKeySpec]:
    """Return single-column foreign keys keyed by local column name."""
    return {
        fk.columns[0]: fk
        for fk in spec.foreign_keys
        if len(fk.columns) == 1 and len(fk.ref_columns) == 1
    }


def _column_type(
    table_name: str,
    column: ColumnSpec,
) -> TypeEngine:
    """Resolve the SQLAlchemy type for a column specification."""
    if column.enum:
        return Enum(*column.enum, name=f'{table_name}_{column.name}_enum')
    return resolve_type(column.type)


def _column_foreign_key(
    column_name: str,
    fk_by_column: dict[str, ForeignKeySpec],
) -> ForeignKey | None:
    """Return a SQLAlchemy single-column foreign key if one exists."""
    col_fk = fk_by_column.get(column_name)
    if col_fk is None:
        return None
    return ForeignKey(
        f'{col_fk.ref_table}.{col_fk.ref_columns[0]}',
        ondelete=col_fk.ondelete,
    )


def _column_kwargs(
    column: ColumnSpec,
    pk_cols: set[str],
) -> dict[str, Any]:
    """Build keyword arguments for ``mapped_column``."""
    kwargs: dict[str, Any] = {
        'nullable': column.nullable,
        'primary_key': column.name in pk_cols and len(pk_cols) == 1,
        'unique': column.unique,
    }
    if column.default:
        kwargs['server_default'] = text(column.default)
    if column.identity:
        kwargs['autoincrement'] = True
    return kwargs


def _table_args_attribute(
    table_args: Sequence[object],
    table_kwargs: dict[str, str],
) -> tuple[object, ...] | None:
    """Return a SQLAlchemy ``__table_args__`` value when needed."""
    if not table_args and not table_kwargs:
        return None
    args_tuple = tuple(table_args)
    return (*args_tuple, table_kwargs) if table_kwargs else args_tuple


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
    base : ``type[DeclarativeBase]``, optional
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
        _append_table_constraints(table_args, spec)
        fk_by_column = _single_column_foreign_keys(spec)

        attrs: dict[str, object] = {'__tablename__': spec.table}

        for col in spec.columns:
            fk_arg = _column_foreign_key(col.name, fk_by_column)
            fk_args = (fk_arg,) if fk_arg else ()
            col_type = _column_type(spec.table, col)
            kwargs = _column_kwargs(col, pk_cols)

            attrs[col.name] = mapped_column(*fk_args, type_=col_type, **kwargs)

            if col.check:
                table_args.append(
                    CheckConstraint(
                        col.check,
                        name=f'ck_{spec.table}_{col.name}',
                    ),
                )

        table_args_attr = _table_args_attribute(table_args, table_kwargs)
        if table_args_attr:
            attrs['__table_args__'] = table_args_attr

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
    base : ``type[DeclarativeBase]``, optional
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
