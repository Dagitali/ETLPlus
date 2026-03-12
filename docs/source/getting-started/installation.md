# Installation

ETLPlus supports Python 3.13 and 3.14.

## Package users

Install the published package from PyPI:

```bash
pip install etlplus
```

That base install includes the common non-native dependencies used by the
project's semi-structured, spreadsheet, and embedded-database handlers.

## Development installs

Install the editable package plus development tooling:

```bash
pip install -e ".[dev]"
```

Install the remaining optional file-format dependencies when you want fuller
local parity with the repository test matrix:

```bash
pip install -e ".[dev,file]"
```

Install just the runtime file extras when you do not need linting and test
tooling:

```bash
pip install -e ".[file]"
```

## Documentation contributors

Install the docs toolchain used by local builds, CI, and Read the Docs:

```bash
pip install -e ".[docs]"
```

From there, build the docs locally with:

```bash
python -m sphinx -T -W --keep-going -b html docs/source docs/build/html
```

Next steps:

- Continue with the {doc}`quickstart <quickstart>`.
- Explore the {doc}`examples guide <../guides/examples>`.
- Browse the {doc}`API reference <../api/index>`.
