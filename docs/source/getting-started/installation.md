# Installation

ETLPlus supports Python 3.13 and 3.14.

```{include} ../../../README.md
:start-after: <!-- docs:getting-started-installation:start -->
:end-before: <!-- docs:getting-started-installation:end -->
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
