#!/usr/bin/env bash
#
# run-command.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for explicit lifecycle command inputs.
#
# Responsibilities
# - Validate that a required command input was provided.
# - Run the caller-provided command in the current working directory.
# - Emit a clear error message tied to the lifecycle input name when missing.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Use this helper only for inputs intentionally documented as shell commands.
#
# References
# - Bash manual: https://www.gnu.org/software/bash/manual/
# - GitHub composite action guide: https://docs.github.com/en/actions/creating-actions/creating-a-composite-action

set -euo pipefail

if [[ -z "$COMMAND_VALUE" ]]; then
  echo "${COMMAND_INPUT_NAME} is required for the ${PHASE} phase." >&2
  exit 2
fi

eval "$COMMAND_VALUE"
