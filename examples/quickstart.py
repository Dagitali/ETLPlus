"""
Quickstart example demonstrating typical data pipeline operations: extract,
transform, validate, and load.
"""

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Literal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Allow direct execution from a source checkout without requiring PYTHONPATH=.
from etlplus.ops import FieldRulesDict  # noqa: E402
from etlplus.ops import extract  # noqa: E402
from etlplus.ops import load  # noqa: E402
from etlplus.ops import transform  # noqa: E402
from etlplus.ops import validate  # noqa: E402

# SECTION: CONSTANTS ======================================================== #


# Extract sample data
DATA_PATH = 'examples/data/sample.json'
OUTPUT_PATH = 'temp/sample_output.json'

type QuickstartStepName = Literal[
    'aggregate',
    'filter',
    'map',
    'select',
    'sort',
]


# SECTION: FUNCTIONS ======================================================== #


# -- Main Logic -- #


def main() -> None:
    """
    Demonstrate :func:`extract`, :func:`transform`, :func:`validate`, and
    :func:`load`.
    """
    data = extract('file', DATA_PATH, file_format='json')

    # Transform: filter and select.
    ops: Mapping[
        QuickstartStepName,
        Mapping[str, Any] | list[str],
    ] = {
        'filter': {'field': 'age', 'op': 'gt', 'value': 25},
        'select': ['name', 'email'],
    }
    transformed = transform(data, ops)

    # Validate the transformed data.
    rules: Mapping[str, FieldRulesDict] = {
        'name': {'type': 'string', 'required': True},
        'email': {'type': 'string', 'required': True},
    }
    result = validate(transformed, rules)
    if not result.get('valid', False):
        print('ValidationDict failed:\n', result)
        return

    # Load to JSON file
    load(transformed, 'file', OUTPUT_PATH, file_format='json')
    print(f'Wrote {OUTPUT_PATH}')


# SECTION: ENTRY POINT ====================================================== #


if __name__ == '__main__':
    main()
