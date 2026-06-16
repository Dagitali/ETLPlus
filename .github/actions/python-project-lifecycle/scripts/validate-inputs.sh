#!/usr/bin/env bash
#
# validate-inputs.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for validating Python project lifecycle action inputs.
#
# Responsibilities
# - Validate that the requested lifecycle phase is supported.
# - Validate boolean inputs consumed by conditional lifecycle phases.
# - Validate required smoke-test inputs before phase-specific steps run.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Keep phase-specific validation here when the check protects multiple
#   lifecycle routes or improves caller-facing diagnostics.
#
# References
# - Bash manual: https://www.gnu.org/software/bash/manual/
# - GitHub composite action guide: https://docs.github.com/en/actions/creating-actions/creating-a-composite-action

set -euo pipefail

case "$PHASE" in
  bootstrap|lint|format-check|test|typecheck|doclint|docs|sbom|security|build|build-upload|upload-dist|download-dist|wheel-smoke|installer-smoke|deploy)
    ;;
  *)
    echo "Unsupported lifecycle phase: $PHASE" >&2
    exit 2
    ;;
esac

for boolean_input in \
  "skip-artifact-upload=$SKIP_ARTIFACT_UPLOAD" \
  "skip-installer-smoke=$SKIP_INSTALLER_SMOKE" \
  "skip-wheel-smoke=$SKIP_WHEEL_SMOKE"; do
  case "${boolean_input#*=}" in
    true|false)
      ;;
    *)
      echo "${boolean_input%%=*} must be \"true\" or \"false\"." >&2
      exit 2
      ;;
  esac
done

wheel_smoke_required=false
if [[ "$PHASE" == 'wheel-smoke' ]]; then
  wheel_smoke_required=true
elif [[ "$PHASE" == 'build-upload' && "$SKIP_WHEEL_SMOKE" != 'true' ]]; then
  wheel_smoke_required=true
fi

if [[ "$wheel_smoke_required" == 'true' && -z "$BEHAVIOR_SMOKE_COMMAND" ]]; then
  echo 'behavior-smoke-command is required unless skip-wheel-smoke is "true".' >&2
  exit 2
fi

installer_smoke_required=false
if [[ "$PHASE" == 'installer-smoke' ]]; then
  installer_smoke_required=true
elif [[ "$PHASE" == 'build-upload' && "$SKIP_INSTALLER_SMOKE" != 'true' ]]; then
  installer_smoke_required=true
fi

if [[ "$installer_smoke_required" == 'true' ]]; then
  if [[ -z "$COMMAND_NAME" ]]; then
    echo 'command-name is required unless skip-installer-smoke is "true".' >&2
    exit 2
  fi
  if [[ -z "$SMOKE_COMMANDS" ]]; then
    echo 'smoke-commands is required unless skip-installer-smoke is "true".' >&2
    exit 2
  fi
fi
