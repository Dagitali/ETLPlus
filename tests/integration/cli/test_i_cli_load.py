"""
:mod:`tests.integration.cli.test_i_cli_load` module.

Integration-scope smoke tests for the ``etlplus load`` CLI command.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFileParser
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliLoad:
    """Smoke tests for the ``etlplus load`` CLI command."""

    def test_load_stdin_to_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        parse_json_file: JsonFileParser,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test loading a STDIN payload into a JSON file target."""
        out_path = tmp_path / 'out.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(
            ('load', str(out_path), '--target-type', 'file'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload.get('status') == 'success'
        assert out_path.exists()
        assert parse_json_file(out_path) == sample_records

    @pytest.mark.parametrize(
        ('env_name', 'backend_label'),
        [
            ('ETLPLUS_TEST_S3_URI', 's3'),
            ('ETLPLUS_TEST_AZURE_BLOB_URI', 'azure-blob'),
        ],
        ids=['s3', 'azure-blob'],
    )
    def test_load_stdin_to_real_remote_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        monkeypatch: pytest.MonkeyPatch,
        env_name: str,
        backend_label: str,
    ) -> None:
        """Test loading STDIN into a real cloud-backed JSON target."""
        del backend_label
        target = real_remote_target_factory(env_name, suffix='load-real')
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            ('load', target.uri, '--target-type', 'file'),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload.get('status') == 'success'
        assert payload.get('message') == f'Data loaded to {target.uri}'
        assert File(target.uri, FileFormat.JSON).read() == sample_records

    def test_load_stdin_to_remote_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        remote_storage_harness: RemoteStorageHarness,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test loading STDIN into a remote JSON file target."""
        target_uri = 's3://bucket/out.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            ('load', target_uri, '--target-type', 'file'),
        )

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload.get('status') == 'success'
        assert payload.get('message') == f'Data loaded to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == sample_records
