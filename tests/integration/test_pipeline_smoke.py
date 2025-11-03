"""
Pipeline smoke test
===================

Runs a minimal file->file pipeline job end-to-end via the CLI to ensure the
"pipeline --run" path works without network dependencies.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path  # noqa: F401 (reserved for potential future use)

from etlplus.cli import main


def test_pipeline_run_file_to_file(
    monkeypatch,
    tmp_path,
    capsys,
):
    """
    Create a temporary JSON file, a temporary pipeline YAML referencing it as a
    file source and a file target, then run the job with the CLI and verify
    the output file is created with the expected contents.
    """

    # Prepare input and output paths
    data_in = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    input_path = tmp_path / 'input.json'
    output_path = tmp_path / 'output.json'

    input_path.write_text(json.dumps(data_in), encoding='utf-8')

    # Minimal pipeline config (file -> file)
    pipeline_yaml = f"""
name: Smoke Test
sources:
  - name: src
    type: file
    format: json
    path: {input_path}
targets:
  - name: dest
    type: file
    format: json
    path: {output_path}
jobs:
  - name: file_to_file_smoke
    extract:
      source: src
    load:
      target: dest
"""

    cfg_path = tmp_path / 'pipeline.yml'
    cfg_path.write_text(pipeline_yaml, encoding='utf-8')

    # Run CLI: etlplus pipeline --config <cfg> --run file_to_file_smoke
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'etlplus',
            'pipeline',
            '--config',
            str(cfg_path),
            '--run',
            'file_to_file_smoke',
        ],
    )
    result = main()
    assert result == 0

    # CLI should have printed a JSON object with status ok
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload.get('status') == 'ok'
    assert isinstance(payload.get('result'), dict)
    assert payload['result'].get('status') == 'success'

    # Output file should exist and match input data
    assert output_path.exists()
    with output_path.open('r', encoding='utf-8') as f:
        out_data = json.load(f)
    assert out_data == data_in
