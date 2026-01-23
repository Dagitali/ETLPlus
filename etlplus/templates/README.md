# `etlplus.templates` Subpackage

Documentation for the `etlplus.templates` subpackage: SQL and DDL template helpers.

- Provides Jinja2 templates for DDL and view generation
- Supports templated SQL for multiple database backends
- Includes helpers for rendering templates with schema metadata

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.templates` Subpackage](#etlplus-templates-subpackage)
    - [Available Templates](#available-templates)
    - [Rendering Templates](#rendering-templates)
    - [Example: Rendering a DDL Template](#example-rendering-a-ddl-template)
    - [See Also](#see-also)

## Available Templates

- `ddl.sql.j2`: Generic DDL (CREATE TABLE) template
- `view.sql.j2`: Generic view creation template

## Rendering Templates

Use the helpers to render templates with your schema or table metadata:

```python
from etlplus.templates import render_template

sql = render_template("ddl.sql.j2", schema=my_schema)
```

## Example: Rendering a DDL Template

```python
from etlplus.templates import render_template

schema = {"name": "users", "columns": [ ... ]}
sql = render_template("ddl.sql.j2", schema=schema)
print(sql)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- DDL template in [ddl.sql.j2](ddl.sql.j2)
- View template in [view.sql.j2](view.sql.j2)
