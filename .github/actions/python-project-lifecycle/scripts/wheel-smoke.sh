#!/usr/bin/env bash
#
# wheel-smoke.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for built-wheel behavior smoke testing.
#
# Responsibilities
# - Create an isolated virtual environment for a built wheel artifact.
# - Install the selected wheel artifact into that environment.
# - Run a caller-provided smoke command with the smoke-test Python interpreter.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Keep this package-artifact oriented: behavior checks should exercise the
#   installed wheel rather than the checked-out editable workspace.
#
# References
# - Python venv documentation: https://docs.python.org/3/library/venv.html
# - Python packaging documentation: https://packaging.python.org/
# - Python wheel format specification: https://packaging.python.org/en/latest/specifications/binary-distribution-format/

set -euo pipefail

if [[ -z "$BEHAVIOR_SMOKE_COMMAND" ]]; then
  echo 'behavior-smoke-command is required for the wheel-smoke phase.' >&2
  exit 2
fi

python -m venv "$SMOKE_VENV_PATH"
if [[ "${RUNNER_OS}" == "Windows" ]]; then
  smoke_python="${SMOKE_VENV_PATH}/Scripts/python.exe"
else
  smoke_python="${SMOKE_VENV_PATH}/bin/python"
fi

"$smoke_python" -m pip install --upgrade pip
"$smoke_python" -m pip install $WHEEL_GLOB
smoke_command="${BEHAVIOR_SMOKE_COMMAND//\{python\}/$smoke_python}"
eval "$smoke_command"
