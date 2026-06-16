#!/usr/bin/env bash
#
# generate-sbom.sh
# ETLPlus
#
# Copyright © 2026 Dagitali LLC. All rights reserved.
#
# Bash helper for Python project SBOM generation.
#
# Responsibilities
# - Run a caller-provided SBOM command when one is configured.
# - Otherwise install the pinned CycloneDX generator in an isolated virtual
#   environment.
# - Generate an SBOM for the active Python environment.
#
# Maintainer Notes
# - This script is executed by the `python-project-lifecycle` composite action.
# - Keep the SBOM generator isolated from project runtime dependencies so SBOM
#   tooling does not change the environment being inspected.
#
# References
# - CycloneDX Python tooling: https://cyclonedx.org/tool-center/python/
# - CycloneDX use cases: https://cyclonedx.org/use-cases/software-bill-of-materials/
# - Python venv documentation: https://docs.python.org/3/library/venv.html

set -euo pipefail

if [[ -n "$SBOM_COMMAND" ]]; then
  eval "$SBOM_COMMAND"
  exit 0
fi

python -m venv "${RUNNER_TEMP}/cyclonedx-bom-venv"
"${RUNNER_TEMP}/cyclonedx-bom-venv/bin/python" -m pip install --upgrade pip
"${RUNNER_TEMP}/cyclonedx-bom-venv/bin/python" \
  -m pip install "cyclonedx-bom==${SBOM_TOOL_VERSION}"
"${RUNNER_TEMP}/cyclonedx-bom-venv/bin/cyclonedx-py" \
  environment "$(python -c 'import sys; print(sys.executable)')" \
  --output-format "$SBOM_OUTPUT_FORMAT" \
  --output-file "$SBOM_OUTPUT_FILE"
