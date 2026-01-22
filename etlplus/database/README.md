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

- Manage connections to supported databases
- Configure engines for different backends

## Schema and DDL Helpers

- Define table schemas and columns
- Generate DDL statements for supported databases

## ORM Utilities

- Map rows to Python objects
- Simple CRUD helpers for tabular data

## Example: Creating a Table

```python
from etlplus.database import Schema, Engine

engine = Engine.connect("sqlite:///example.db")
schema = Schema.from_dict({"name": "users", "columns": [ ... ]})
engine.create_table(schema)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Schema helpers in [schema.py](schema.py)
- ORM utilities in [orm.py](orm.py)
