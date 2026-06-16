#!/usr/bin/env bash
#
# build-docs.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for Sphinx documentation builds.
#
# Responsibilities
# - Remove stale generated API documentation when the directory exists.
# - Clear prior output for the selected Sphinx builder.
# - Build the selected Sphinx target with warnings treated as errors.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Keep docs paths configurable so the lifecycle action can be reused outside
#   ETLPlus without depending on a local docs action.
#
# References
# - Sphinx documentation: https://www.sphinx-doc.org/
# - Sphinx builders reference: https://www.sphinx-doc.org/en/master/usage/builders/index.html
# - GitHub composite action guide: https://docs.github.com/en/actions/creating-actions/creating-a-composite-action

set -euo pipefail

for required_input in \
  "docs-builder=$DOCS_BUILDER" \
  "docs-build-dir=$DOCS_BUILD_DIR" \
  "docs-doctree-dir=$DOCS_DOCTREE_DIR" \
  "docs-source-dir=$DOCS_SOURCE_DIR"; do
  if [[ -z "${required_input#*=}" ]]; then
    echo "${required_input%%=*} is required for the docs phase." >&2
    exit 2
  fi
done

if [[ ! -d "$DOCS_SOURCE_DIR" ]]; then
  echo "docs-source-dir does not exist: $DOCS_SOURCE_DIR" >&2
  exit 2
fi

if [[ -d "$DOCS_GENERATED_API_DIR" ]]; then
  find "$DOCS_GENERATED_API_DIR" \
    -type f \
    -name '*.rst' \
    -delete
fi

rm -rf \
  "${DOCS_BUILD_DIR}/${DOCS_BUILDER}" \
  "${DOCS_DOCTREE_DIR}/${DOCS_BUILDER}"

python -m sphinx -T -W --keep-going \
  -b "$DOCS_BUILDER" \
  -d "${DOCS_DOCTREE_DIR}/${DOCS_BUILDER}" \
  "$DOCS_SOURCE_DIR" \
  "${DOCS_BUILD_DIR}/${DOCS_BUILDER}"
