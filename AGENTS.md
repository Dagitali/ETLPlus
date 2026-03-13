# AGENTS

## Release Readiness

When a task affects release preparation, packaging, versioned docs, or CI, consult
`RELEASE-CHECKLIST.md` before making changes.

Agent rules:

- Treat `pyproject.toml` as the canonical packaging metadata source.
- Keep `setup.py` as a minimal compatibility shim unless the user explicitly asks to restore
  setuptools-managed metadata there.
- Do not place internal planning or release-gating notes under `docs/source/` unless they are meant
  to be published on Read the Docs.
- Prefer repository-root or `.github/` locations for internal process documents.
- When changing release automation or packaging, preserve these checks unless the user explicitly
  wants to relax them:
  - build sdist and wheel
  - run `twine check`
  - smoke-test the built wheel in a clean environment
- When changing versioned docs or install snippets, refresh generated demo/version snippets if the
  change affects published version output.
- Do not make docstring-lint or full type-check gates blocking in CI unless the existing repository
  backlog has been intentionally addressed.
- If a change narrows or expands the stable public surface for `v1.0.0`, update
  `RELEASE-CHECKLIST.md` to reflect that decision.
