# Contributing

The full contribution guide lives in the repository's
[`CONTRIBUTING.md`](https://github.com/Dagitali/ETLPlus/blob/main/CONTRIBUTING.md).
This page highlights the parts that matter most when you are improving the
published docs or the API reference.

## Documentation expectations

- Use NumPy-style docstrings for public APIs.
- Keep structured `Parameters`, `Returns`, and `Raises` sections for public
  functions and methods.
- Do not collapse docstrings to one-liners just because a function is a thin
  wrapper.
- Keep examples and guides aligned with the current CLI and Python API.

## Typing and API shape

The project prefers permissive runtime behavior with strong editor support:

- `TypedDict` shapes are primarily for tooling and autocomplete.
- Constructors commonly accept `Mapping[str, Any]` and perform tolerant parsing.
- Unknown keys should not be rejected without a strong reason.

For the full rationale, see the
[`Typing Philosophy`](https://github.com/Dagitali/ETLPlus/blob/main/CONTRIBUTING.md#typing-philosophy)
section.

## Testing expectations

- Unit tests belong under `tests/unit/`.
- Integration tests belong under `tests/integration/`.
- End-to-end tests belong under `tests/e2e/`.
- Tests that call `etlplus.cli.main()` or `etlplus.ops.run.run()` are
  integration tests by default.

For contributor-friendly local parity:

```bash
pip install -e ".[dev,file,docs]"
pytest
python -m sphinx -T -W --keep-going -b html docs/source docs/build/html
```

## Community and support

- [`CODE_OF_CONDUCT.md`](https://github.com/Dagitali/ETLPlus/blob/main/CODE_OF_CONDUCT.md)
- [`SECURITY.md`](https://github.com/Dagitali/ETLPlus/blob/main/SECURITY.md)
- [`SUPPORT.md`](https://github.com/Dagitali/ETLPlus/blob/main/SUPPORT.md)
