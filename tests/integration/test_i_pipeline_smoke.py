"""
:mod:`tests.integration.test_i_pipeline_smoke` module.

Pipeline smoke integration test suite exercising a minimal file→file job via
the CLI (using the deprecated-free path). Parametrized to verify both empty
and non-empty inputs.

Notes
-----
- Builds a transient pipeline YAML string per test run.
- Invokes ``etlplus run --job <job>`` end-to-end.
- Validates output file contents against input data shape.
"""

from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFactory

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


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
        tmp_path: Path,
        json_file_factory: JsonFactory,
        cli_invoke: CliInvoke,
        data_in: list[object] | list[dict[str, int | str]],
    ) -> None:
        """Test file→file jobs via CLI for multiple input datasets."""
        source_path = json_file_factory(data_in, filename='input.json')
        output_path = tmp_path / 'output.json'

        # Minimal pipeline config (file -> file).
        pipeline_yaml = dedent(
            f"""
            name: Smoke Test
            sources:
              - name: src
                type: file
                format: json
                path: "{source_path}"
            targets:
              - name: dest
                type: file
                format: json
                path: "{output_path}"
            jobs:
              - name: file_to_file_smoke
                extract:
                  source: src
                load:
                  target: dest
            """,
        ).strip()
        cfg_path = tmp_path / 'pipeline.yml'
        cfg_path.write_text(pipeline_yaml, encoding='utf-8')

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg_path),
                '--job',
                'file_to_file_smoke',
            ),
        )
        assert err == ''
        assert code == 0

        payload = json.loads(out)

        # CLI should have printed a JSON object with status ok.
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'

        # Output file should exist and match input data.
        assert output_path.exists()
        with output_path.open('r', encoding='utf-8') as f:
            out_data = json.load(f)
        assert out_data == data_in
