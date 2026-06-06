# Documentation Notes

ETLPlus keeps published user docs, package-level README files, and internal maintainer notes in
separate locations so each document has a clear audience. Use this guide when deciding where a new
document belongs or when updating generated snippets before a release.

Published Read the Docs pages should describe stable user behavior, supported install paths, and
public package facades. Internal planning, release-gating notes, and future design sketches should
stay at the repository root or under `.github/` unless maintainers intentionally promote them to the
published docs.

- [Published Docs](#published-docs)
- [Package READMEs](#package-readmes)
- [Internal Notes](#internal-notes)
- [Generated Snippets](#generated-snippets)
- [Local Validation](#local-validation)

## Published Docs

Place user-facing guides under `docs/source/` when the content should appear on Read the Docs.
Prefer stable command examples, public imports, and package install commands such as:

```bash
pip install "etlplus[storage]"
```

Use editable installs only in contributor-focused sections where the reader is expected to work from
a local checkout.

## Package READMEs

Package-level README files under `etlplus/*/README.md` should explain the supported facade for that
subpackage. Link to `__init__.py` or public docs when describing supported imports, and reserve
underscore-prefixed module references for clearly labeled implementation-layout notes.

## Internal Notes

Keep maintainer process documents, release checklists, and forward-looking design notes outside
`docs/source/` unless they are intended to be public. Root-level internal notes should start with a
short status line so maintainers can tell whether the document is public, internal, archived, or
release-facing.

## Generated Snippets

Some published pages include marked sections from the root README or generated version snippets.
When install output or version text changes, refresh those snippets before tagging:

```bash
python tools/update_demo_snippets.py
```

Review the resulting `DEMO.md` and `docs/snippets/installation_version.md` changes before including
them in a release branch.

## Local Validation

For documentation contributors:

```bash
pip install -e ".[docs]"
make docs-strict
```

For release-facing documentation changes, also consult `RELEASE-CHECKLIST.md` before tagging or
publishing.
