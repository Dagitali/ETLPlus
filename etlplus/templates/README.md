# `etlplus.templates` Subpackage

Documentation for the `etlplus.templates` subpackage: bundled SQL/DDL templates used by the database
helpers.

- Provides Jinja2 templates for DDL and view generation
- Used by `etlplus.database.render_table_sql` and related helpers
- Exposed as plain template files you can reuse with your own Jinja2 setup

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

ETLPlus does not currently expose a `render_template` helper in this package. Use the database
helpers instead:

```python
from etlplus.database import render_table_sql, load_table_spec

spec = load_table_spec("schemas/users.yml")
sql = render_table_sql(spec, template="ddl")
```

## Example: Rendering a DDL Template

```python
from etlplus.database import render_tables_to_string

sql = render_tables_to_string(["schemas/users.yml"], template="ddl")
print(sql)
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- DDL template in [ddl.sql.j2](ddl.sql.j2)
- View template in [view.sql.j2](view.sql.j2)
