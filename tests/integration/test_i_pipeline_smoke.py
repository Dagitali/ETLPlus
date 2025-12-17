"""
:mod:`tests.integration.test_i_pipeline_smoke` module.

Pipeline smoke integration test suite exercising a minimal file→file job via
the CLI. Parametrized to verify both empty and non-empty inputs.

Notes
-----
- Builds a transient pipeline YAML string per test run.
- Invokes ``etlplus pipeline --run <job>`` end-to-end.
- Validates output file contents against input data shape.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from etlplus.cli import main

# SECTION: TESTS ============================================================ #


class TestPipelineSmoke:
    """Integration test suite for file→file job via CLI."""

    @pytest.mark.parametrize(
        'data_in',
        [
            [],
            [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
        ],
        ids=['empty', 'two-records'],
    )
    def test_file_to_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        data_in: list[object] | list[dict[str, int | str]],
    ) -> None:
        """Test file→file jobs via CLI for multiple input datasets."""
        # Prepare input and output paths.
        input_path = tmp_path / 'input.json'
        output_path = tmp_path / 'output.json'
        input_path.write_text(json.dumps(data_in), encoding='utf-8')

        # Minimal pipeline config (file -> file).
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

        # Run CLI: etlplus pipeline --config <cfg> --run file_to_file_smoke.
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

        payload = json.loads(capsys.readouterr().out)

        # CLI should have printed a JSON object with status ok.
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'

        # Output file should exist and match input data.
        assert output_path.exists()
        with output_path.open('r', encoding='utf-8') as f:
            out_data = json.load(f)
        assert out_data == data_in
