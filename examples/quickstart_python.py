from collections.abc import Mapping
from typing import cast

from etlplus import extract
from etlplus import load
from etlplus import transform
from etlplus import validate
from etlplus.types import PipelineConfig
from etlplus.validate import FieldRules

# Extract sample data
DATA_PATH = 'examples/data/sample.json'
OUTPUT_PATH = 'examples/sample_output.json'


def main() -> None:
    data = extract('file', DATA_PATH, file_format='json')

    # Transform: filter and select.
    ops = cast(
        PipelineConfig,
        {
            'filter': {'field': 'age', 'op': 'gt', 'value': 25},
            'select': ['name', 'email'],
        },
    )
    transformed = transform(data, ops)

    # Validate the transformed data.
    rules = cast(
        Mapping[str, FieldRules],
        {
            'name': {'type': 'string', 'required': True},
            'email': {'type': 'string', 'required': True},
        },
    )
    result = validate(transformed, rules)
    if not result.get('valid', False):
        print('Validation failed:\n', result)
        return

    # Load to JSON file
    load(transformed, 'file', OUTPUT_PATH, file_format='json')
    print(f'Wrote {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
