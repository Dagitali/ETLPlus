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

- TypedDicts in `etlplus/config/types.py` are editor/type-checking hints.  They are intentionally
  `total=False` (all keys optional) and are not enforced at runtime.
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

[Code of Conduct]: CODE_OF_CONDUCT.md
[owner]: https://dagitali.com
[pull request]: https://github.com/Dagitali/ETLPlus/pulls
[README]: README.md
[SemVer]: http://semver.org
