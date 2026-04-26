"""
:mod:`smoke_test_schema_validation` module.

Smoke-test schema validation through an installed ``etlplus`` CLI.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _run_validation(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f'Validation command did not emit JSON: {command!r}\n'
            f'STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}',
        ) from exc

    if not payload.get('valid'):
        raise RuntimeError(
            f'Validation command reported invalid data: {command!r}\n'
            f'STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}',
        )


# SECTION: FUNCTIONS ======================================================== #


def main(argv: list[str]) -> int:
    """
    Run schema-validation smoke checks through one installed CLI executable.

    Parameters
    ----------
    argv : list[str]
        Command-line arguments (excluding the executable).
    """
    repo_root = Path(__file__).resolve().parents[1]
    python_executable = Path(argv[0]) if argv else Path(sys.executable)

    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        csv_path = temp_dir / 'sample.csv'
        frictionless_schema_path = temp_dir / 'schema.json'
        json_path = temp_dir / 'customer.json'
        csv_path.write_text('name,age\nAda,37\n', encoding='utf-8')
        frictionless_schema_path.write_text(
            '{"fields": [{"name": "name", "type": "string"}, '
            '{"name": "age", "type": "integer"}]}',
            encoding='utf-8',
        )
        json_path.write_text(
            '{"name": "Ada", "email": "ada@example.com"}',
            encoding='utf-8',
        )

        _run_validation(
            [
                str(python_executable),
                '-m',
                'etlplus',
                'validate',
                str(repo_root / 'examples/data/sample.xml'),
                '--schema',
                str(repo_root / 'examples/data/sample.xsd'),
            ],
        )
        _run_validation(
            [
                str(python_executable),
                '-m',
                'etlplus',
                'validate',
                str(json_path),
                '--schema',
                str(repo_root / 'examples/schemas/customer.schema.json'),
            ],
        )
        _run_validation(
            [
                str(python_executable),
                '-m',
                'etlplus',
                'validate',
                str(csv_path),
                '--schema',
                str(frictionless_schema_path),
            ],
        )

    print('schema validation smoke test: ok')
    return 0


# SECTION: MAIN ENTRY POINT ====================================================== #

if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
