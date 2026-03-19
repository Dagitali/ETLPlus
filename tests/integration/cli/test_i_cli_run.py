"""
:mod:`tests.integration.cli.test_i_cli_run` module.

Integration-scope smoke test suite for a minimal file→file CLI job.

Notes
-----
- Builds a transient pipeline YAML string per test run.
- Invokes ``etlplus run --job <job>`` end-to-end.
- Validates output file contents against input data shape.
"""

from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFileParser
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import PipelineConfigFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestRun:
    """Smoke tests for file→file job via CLI."""

    def test_file_to_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        parse_json_file: JsonFileParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
    ) -> None:
        """Test that file→file jobs run via CLI for multiple input datasets."""
        cfg = pipeline_config_factory(sample_records)

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
            ),
        )
        assert err == ''
        assert code == 0

        payload = parse_json_output(out)

        # CLI should have printed a JSON object with status ok.
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'

        # Output file should exist and match input data.
        assert cfg.output_path.exists()
        out_data = parse_json_file(cfg.output_path)
        assert out_data == sample_records

    def test_remote_file_to_remote_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        remote_storage_harness: RemoteStorageHarness,
        tmp_path: Path,
    ) -> None:
        """Test that remote file jobs run via CLI with file options."""
        source_uri = 's3://bucket/input.csv'
        target_uri = 's3://bucket/output.csv'
        remote_storage_harness.set_text(
            source_uri,
            'name|age\nAda|36\nGrace|47\n',
        )
        job_name = 'remote_file_to_file_smoke'
        cfg_path = tmp_path / 'remote_pipeline.yml'
        cfg_path.write_text(
            dedent(
                f'''
                name: Remote Smoke Test
                sources:
                  - name: src
                    type: file
                    format: csv
                    path: "{source_uri}"
                    options:
                      delimiter: ","
                      encoding: "utf-8"
                targets:
                  - name: dest
                    type: file
                    format: csv
                    path: "{target_uri}"
                    options:
                      delimiter: ","
                      encoding: "utf-8"
                jobs:
                  - name: {job_name}
                    extract:
                      source: src
                      options:
                        delimiter: "|"
                    load:
                      target: dest
                      overrides:
                        delimiter: ";"
                ''',
            ).strip(),
            encoding='utf-8',
        )

        code, out, err = cli_invoke(
            ('run', '--config', str(cfg_path), '--job', job_name),
        )

        assert err == ''
        assert code == 0
        payload = parse_json_output(out)
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'
        assert (
            payload['result'].get('message')
            == f'Data loaded to {target_uri}'
        )
        rows = list(
            csv.DictReader(
                StringIO(remote_storage_harness.read_text(target_uri)),
                delimiter=';',
            ),
        )
        assert rows == [
            {'age': '36', 'name': 'Ada'},
            {'age': '47', 'name': 'Grace'},
        ]
