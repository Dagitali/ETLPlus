# `etlplus.database` Subpackage

Documentation for the `etlplus.database` subpackage: database engine, schema, and ORM helpers.

- Provides database engine and connection management
- Supports schema definition and DDL generation
- Includes lightweight ORM utilities for tabular data
- Exposes type definitions for database objects

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.database` Subpackage](#etlplusdatabase-subpackage)
  - [Database Engine and Connections](#database-engine-and-connections)
  - [Schema and DDL Helpers](#schema-and-ddl-helpers)
  - [ORM Utilities](#orm-utilities)
  - [Example: Creating a Table](#example-creating-a-table)
  - [See Also](#see-also)

## Database Engine and Connections

- Build SQLAlchemy engines with `make_engine`
- Load connection strings from pipeline configs

## Schema and DDL Helpers

- Define table schemas and columns
- Generate DDL statements for supported databases

## ORM Utilities

- Map rows to Python objects
- Simple CRUD helpers for tabular data

## Example: Rendering DDL From a Spec

```python
from etlplus.database import load_table_spec, render_table_sql

spec = load_table_spec("schemas/users.yml")
sql = render_table_sql(spec, template="ddl")
print(sql)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Schema helpers in [schema.py](schema.py)
- ORM utilities in [orm.py](orm.py)
