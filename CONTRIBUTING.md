# Contributing Guidelines

Version-controllable contributions toward improving [this project][README] (its source code,
documentation, etc.) are welcome via GitHub's [pull request] process.  By submitting a merge
request, you acknowledge and agree to licensing your contribution to
[Dagitali LLC][owner].

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change.

- [Contributing Guidelines](#contributing-guidelines)
  - [Merge Request Process](#merge-request-process)
  - [Code of Conduct](#code-of-conduct)
  - [Type Checking](#type-checking)
  - [Typing Philosophy](#typing-philosophy)
  - [Documentation Style](#documentation-style)
  - [Testing](#testing)
    - [Scope and Intent](#scope-and-intent)
    - [Test Configuration](#test-configuration)
    - [Running Tests](#running-tests)
    - [Common Patterns](#common-patterns)

## Merge Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a
   build.
2. Update the [README] with details of changes to the interface.  This includes new environment
   variables, exposed ports, useful file locations and container parameters.
3. Increase the version numbers in any examples files and the [README] to the new version that this
   pull request would represent.  The versioning scheme we use is [SemVer].
4. You may merge in the merge request once you have the sign-off of two other developers.  If you do
   not have permission to do that, you may request the second reviewer to merge it for you.

## Code of Conduct

All contributors are expected to honor and adhere to our [Code of Conduct] policy.  As such, please
do the following:

1. Read it before making any contributions;
2. Follow it in all your interactions with the project.

## Type Checking

We ship a `py.typed` marker so downstream consumers get typing by default.  For local development,
if your editor reports missing stubs for `requests` (e.g., "Library stubs not installed for
requests"), either install the types package or rely on inline ignores present in the codebase:

```bash
pip install types-requests
```

We’ve added `# type: ignore` on imports where appropriate to keep editors happy without extra deps,
but using the stubs can provide a nicer editing experience.

## Typing Philosophy

We optimize for a great editor experience and a permissive runtime:

- TypedDicts in `etlplus/*/types.py` (for example `etlplus/api/types.py`) are editor/type-checking
  hints. They are intentionally `total=False` (all keys optional) and are not enforced at runtime.
- Constructors named `*.from_obj` accept `Mapping[str, Any]` and perform tolerant parsing and light
  casting.  This keeps runtime permissive while improving autocomplete and static analysis.
- Prefer `Mapping[str, Any]` for inputs and plain `dict[...]` for internal state/returns.  Avoid
  tight coupling to simple alias types.
- Use Python 3.13 conveniences when helpful: `Self` return type in classmethods, dict union
  operators (`|`, `|=`), and modern typing.
- Provide `@overload` signatures to narrow inputs (e.g., `str` vs Mapping; or specific `TypedDict`
  shapes).  Import these shape types only under `TYPE_CHECKING` to avoid runtime import cycles.
- Keep behavior backward compatible and permissive: do not reject unknown keys; pass through
  provider-specific blocks as `Mapping[str, Any]`.
- Tests: add small, focused unit tests for new constructors and merge logic (happy path plus 1–2
  edge cases).

Example (type-only import + overload):

```py
from typing import TYPE_CHECKING, overload
from collections.abc import Mapping
from typing import Any, Self

if TYPE_CHECKING:
      from .types import ExampleConfigMap

class ExampleConfig:
      @classmethod
      @overload
      def from_obj(cls, obj: 'ExampleConfigMap') -> Self: ...

      @classmethod
      @overload
      def from_obj(cls, obj: Mapping[str, Any]) -> Self: ...

      @classmethod
      def from_obj(cls, obj: Mapping[str, Any]) -> Self:
            if not isinstance(obj, Mapping):
                  raise TypeError('ExampleConfig must be a mapping')
            # parse fields permissively here
            ...
```

## Documentation Style

Use NumPy-style docstrings for public APIs.

- Keep full sections for public functions and methods: `Parameters`, `Returns`, and `Raises` when
  applicable.
- Do not collapse public API docstrings to one-liners solely because the function is a thin
  delegator.
- For deprecated wrappers, include an explicit deprecation sentence near the top, while still
  keeping the full structured sections.
- For `etlplus/file` module-level wrappers (`read`/`write`) specifically, preserve full
  `Parameters` and `Returns` sections.

[Code of Conduct]: CODE_OF_CONDUCT.md
[owner]: https://dagitali.com
[pull request]: https://github.com/Dagitali/ETLPlus/pulls
[README]: README.md
[SemVer]: http://semver.org

## Testing

### Scope and Intent

Use these guidelines to decide where tests live and how to label intent.

- Unit tests (put under `tests/unit/`):
  - Exercise a single function or class directly in isolation (no orchestration across modules).
  - Avoid real file system or network I/O; use `tmp_path` for local files and stubs/mocks for external calls.
  - Fast and deterministic; rely on `monkeypatch` to stub collaborators.
  - Examples in this repo:
    - Small helpers in `etlplus.utils`
    - Validation and transform functions.

- Integration tests (put under `tests/integration/`):
  - Exercise end-to-end flows across modules and boundaries.
  - Can use CLI argv, temporary files/directories, and stub network with fakes/mocks.
  - Examples in this repo:
    - CLI `main()` end-to-end
    - `run()` pipeline orchestration
    - File connectors
    - API client pagination wiring/strategy
    - Runner defaults for pagination/rate limits
    - Target URL composition.

- E2E tests (put under `tests/e2e/`):
  - Validate full system-boundary workflows.
  - Keep slower, higher-scope checks here.

- Smoke tests are an intent marker, not a scope folder:
  - Place smoke tests in `tests/unit/`, `tests/integration/`, or `tests/e2e/`
    based on scope.
  - Mark them with `@pytest.mark.smoke`.
  - `tests/smoke/` is a transitional legacy path during migration.

If a test calls `etlplus.cli.main()` or `etlplus.ops.run.run()`, it is integration by default.

### Test Configuration

- Each test folder should include a `conftest.py` for shared fixtures.
- Use scope markers (`unit`, `integration`, `e2e`) and intent markers
  (`smoke`, `contract`) from `pytest.ini`.
- Add `@pytest.mark.smoke` and `@pytest.mark.contract` directly on modules/tests
  where intent applies.
- Markers are declared in `pytest.ini`. Avoid introducing ad-hoc markers without adding them there.
- For optional dependencies, prefer `pytest.importorskip("module")` so tests skip cleanly when the extra isn’t installed.

### Running Tests

Common commands:

- Run everything:
  - `pytest`
- Run a specific suite:
  - `pytest -m unit`
  - `pytest -m integration`
  - `pytest -m e2e`
  - `pytest -m smoke`
  - `pytest -m contract`
- Run a specific file or test:
  - `pytest tests/unit/file/test_u_file_core.py`
  - `pytest tests/unit/file/test_u_file_core.py::TestFile::test_roundtrip_by_format`
- Run by keyword:
  - `pytest -k "roundtrip"`

### Common Patterns

- CLI tests: monkeypatch `sys.argv` and call `etlplus.cli.main()`; capture output with `capsys`.
- File I/O: use `tmp_path` / `TemporaryDirectory()`; never write to the repo tree.
- API flows: stub `EndpointClient` or transport layer via `monkeypatch` to avoid real HTTP.
- Runner tests: monkeypatch `load_config` to inject an in-memory `Config`.
- Keep tests small and focused; prefer one behavior per test with clear assertions.
