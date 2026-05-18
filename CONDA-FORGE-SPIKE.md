# Conda-Forge Feasibility Spike

Status: Internal packaging spike. Do not treat conda-forge as a supported ETLPlus install channel
until maintainers decide to open and maintain a feedstock.

Date: 2026-05-18

- [Conda-Forge Feasibility Spike](#conda-forge-feasibility-spike)
  - [Result](#result)
  - [Dependency Mapping](#dependency-mapping)
  - [Candidate Recipe](#candidate-recipe)
  - [Feedstock Preparation](#feedstock-preparation)
  - [Commands Run](#commands-run)
  - [Recipe Build Validation](#recipe-build-validation)
  - [Notes](#notes)
  - [Recommended Next Step](#recommended-next-step)

## Result

The initial conda-forge path is viable for the current ETLPlus base dependency set on `osx-arm64`
with Python 3.13.

Validated locally:

- Full base dependency solve from conda-forge with Python 3.13.
- Source install into a conda prefix with conda-resolved dependencies and pip build isolation
  disabled.
- Rendered recipe build/test from a clean local source snapshot with `conda-build` on `osx-arm64`.
- Console entrypoint smoke checks:
  - `etlplus --version`
  - `etlplus --help`
  - `etlplus check --help`
  - `python -c "import etlplus; print(etlplus.__version__)"`

Not validated in this spike:

- A tagged PyPI sdist recipe build with a pinned release `sha256`.
- Cross-platform solves beyond the current `osx-arm64` environment.
- Optional extras as conda outputs or variants.

## Dependency Mapping

Most PyPI dependency names map directly to conda-forge package names. Required differences:

| PyPI requirement | Conda-forge requirement |
| --- | --- |
| `msgpack>=1.0.8` | `msgpack-python >=1.0.8` |
| `PyYAML>=6.0.3` | `pyyaml >=6.0.3` |
| `SQLAlchemy>=2.0.45` | `sqlalchemy >=2.0.45` |

The broad base runtime solve succeeded with the current default dependency posture, including
`frictionless >=5.19.0`, `pyarrow >=22.0.0`, `pandas >=2.3.3`, `duckdb`, and `pyodbc >=5.3.0`.

## Candidate Recipe

Prefer an initial pure-Python package recipe with conda-forge resolving compiled dependencies.
Optional extras should stay out of the first feedstock pass unless maintainers decide to publish
separate outputs or variants.

```yaml
{% set name = "etlplus" %}
{% set version = "<release-version>" %}

package:
  name: {{ name|lower }}
  version: {{ version }}

source:
  url: https://pypi.org/packages/source/e/etlplus/etlplus-{{ version }}.tar.gz
  sha256: <sdist-sha256>

build:
  noarch: python
  number: 0
  script: {{ PYTHON }} -m pip install . --no-deps --no-build-isolation -vv
  entry_points:
    - etlplus = etlplus.cli:main

requirements:
  host:
    - python >=3.13,<3.15
    - pip
    - setuptools >=80
    - setuptools-scm >=8
    - wheel
  run:
    - python >=3.13,<3.15
    - cbor2 >=5.6.4
    - duckdb >=1.1.0
    - fastavro >=1.12.1
    - frictionless >=5.19.0
    - jinja2 >=3.1.6
    - jsonschema >=4.26.0
    - lxml >=6.1.0
    - msgpack-python >=1.0.8
    - odfpy >=1.4.1
    - openpyxl >=3.1.5
    - pyodbc >=5.3.0
    - pyarrow >=22.0.0
    - pymongo >=4.9.1
    - python-dotenv >=1.2.1
    - pandas >=2.3.3
    - pydantic >=2.12.5
    - pyyaml >=6.0.3
    - requests >=2.32.5
    - sqlalchemy >=2.0.45
    - tomli-w >=1.2.0
    - typer >=0.21.0
    - xlrd >=2.0.2
    - xlwt >=1.3.0

test:
  imports:
    - etlplus
  commands:
    - etlplus --version
    - etlplus --help
    - etlplus check --help
  requires:
    - pip

about:
  home: https://github.com/Dagitali/ETLPlus
  summary: A Swiss Army knife for simple ETL operations
  license: MIT
  license_file: LICENSE
  doc_url: https://etlplus.readthedocs.io/en/stable/
  dev_url: https://github.com/Dagitali/ETLPlus

extra:
  recipe-maintainers:
    - <maintainer-github-handle>
```

## Feedstock Preparation

The candidate recipe has been promoted into a draft feedstock-preparation template at
`packaging/conda/meta.yaml.j2`, with local validation notes in `packaging/conda/README.md`.

The draft recipe intentionally keeps release-specific values as placeholders:

- `<release-version>`
- `<sdist-sha256>`
- `<maintainer-github-handle>`

Before submission to conda-forge, render those placeholders from the tagged PyPI sdist with
`tools/render_conda_recipe.py`, repeat the recipe build/test from that release artifact, and verify
the result on Linux, macOS, and Windows.

The manual `Conda Recipe Validation` GitHub Actions workflow provides the non-release validation
path for the draft recipe. It runs Linux by default and can be dispatched with `platform_scope: all`
to include macOS and Windows before maintainers decide whether to support conda-forge.

## Commands Run

Dry-run solve:

```bash
mamba create --dry-run --yes --name etlplus-conda-spike --channel conda-forge \
  python=3.13 cbor2 duckdb fastavro 'frictionless>=5.19.0' jinja2 \
  'jsonschema>=4.26.0' 'lxml>=6.1.0' 'msgpack-python>=1.0.8' odfpy openpyxl \
  'pyodbc>=5.3.0' 'pyarrow>=22.0.0' pymongo 'python-dotenv>=1.2.1' \
  'pandas>=2.3.3' 'pydantic>=2.12.5' 'pyyaml>=6.0.3' 'requests>=2.32.5' \
  'sqlalchemy>=2.0.45' tomli-w 'typer>=0.21.0' xlrd xlwt
```

Temporary prefix install:

```bash
mamba create --yes --prefix /private/tmp/etlplus-conda-spike --channel conda-forge \
  python=3.13 pip 'setuptools>=80' 'setuptools-scm>=8' wheel \
  cbor2 duckdb fastavro 'frictionless>=5.19.0' jinja2 'jsonschema>=4.26.0' \
  'lxml>=6.1.0' 'msgpack-python>=1.0.8' odfpy openpyxl 'pyodbc>=5.3.0' \
  'pyarrow>=22.0.0' pymongo 'python-dotenv>=1.2.1' 'pandas>=2.3.3' \
  'pydantic>=2.12.5' 'pyyaml>=6.0.3' 'requests>=2.32.5' \
  'sqlalchemy>=2.0.45' tomli-w 'typer>=0.21.0' xlrd xlwt

/private/tmp/etlplus-conda-spike/bin/python -m pip install --no-deps --no-build-isolation .
/private/tmp/etlplus-conda-spike/bin/etlplus --version
/private/tmp/etlplus-conda-spike/bin/etlplus --help
/private/tmp/etlplus-conda-spike/bin/etlplus check --help
/private/tmp/etlplus-conda-spike/bin/python -c "import etlplus; print(etlplus.__version__)"
```

Recipe render and local source build:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /private/tmp/etlplus-conda-clean/meta.yaml \
  --version 0.0.0 \
  --sha256 0000000000000000000000000000000000000000000000000000000000000000 \
  --maintainer dagitali-maintainer \
  --source-path /private/tmp/etlplus-clean-src

conda-build /private/tmp/etlplus-conda-clean \
  --channel conda-forge \
  --output-folder /private/tmp/etlplus-conda-build-output-clean
```

## Recipe Build Validation

The rendered local recipe built successfully with `conda-build` on `osx-arm64` from a clean
`git archive` source snapshot. The resulting package artifact was:

```text
/private/tmp/etlplus-conda-build-output-clean/noarch/etlplus-0.0.0-py_0.conda
```

The recipe test phase installed the artifact in a clean conda test environment and passed:

- `etlplus --version`
- `etlplus --help`
- `etlplus check --help`

A follow-up `conda-build --python 3.13` run also passed, but the noarch recipe still resolved the
host/test interpreter to Python 3.14 in this local environment. Treat the earlier direct
`python=3.13` solve and source-install checks as the lower-bound Python evidence, not this noarch
variant run.

## Notes

- `mamba run` was not usable in the sandbox because it tried to write a process lock under the home
  cache. Direct prefix executables worked.
- The direct local source install produced a development version from `setuptools-scm`; the clean
  `git archive` recipe build used the `0.0.0` fallback because archive snapshots do not carry git
  metadata. A released conda-forge recipe should use the tagged PyPI sdist and pinned `sha256`.
- The first feedstock should keep conda-forge as an alternate packaging channel for the same broad
  base runtime contract, not as a dependency-splitting change.

## Recommended Next Step

If maintainers accept the support burden, render `packaging/conda/meta.yaml.j2` from a tagged PyPI
sdist, then repeat the recipe build/test locally or in CI on Linux, macOS, and Windows before
submitting to staged-recipes.
