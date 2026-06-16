#!/usr/bin/env bash
#
# build-distributions.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for Python distribution build and validation phases.
#
# Responsibilities
# - Run the caller-provided Python distribution build command.
# - Optionally run the caller-provided release artifact audit command.
# - Optionally validate built distributions with `twine check`.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Caller release workflows should keep distribution builds, artifact audits,
#   and twine validation enabled unless their release policy intentionally
#   changes.
#
# References
# - Python packaging build frontend: https://build.pypa.io/
# - Python packaging documentation: https://packaging.python.org/
# - Twine documentation: https://twine.readthedocs.io/

set -euo pipefail

eval "$BUILD_COMMAND"

if [[ -n "$RELEASE_ARTIFACT_AUDIT_COMMAND" ]]; then
  eval "$RELEASE_ARTIFACT_AUDIT_COMMAND"
else
  echo 'Skipping release artifact audit.'
fi

case "$TWINE_CHECK" in
  true)
    python -m twine check $DIST_GLOB
    ;;
  false)
    echo 'Skipping twine check.'
    ;;
  *)
    echo 'twine-check must be "true" or "false".' >&2
    exit 2
    ;;
esac
