"""
:mod:`tests.integration.cli.test_i_cli_validate` module.

Integration-scope smoke tests for the ``etlplus validate`` CLI command.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliValidate:
    """Smoke tests for the ``etlplus validate`` CLI command."""

    def test_stdin_payload(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating a STDIN payload with basic rules."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(('validate', '--rules', rules_json, '-'))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True

    @pytest.mark.parametrize(
        ('env_name', 'backend_label'),
        [
            ('ETLPLUS_TEST_S3_URI', 's3'),
            ('ETLPLUS_TEST_AZURE_BLOB_URI', 'azure-blob'),
        ],
        ids=['s3', 'azure-blob'],
    )
    def test_stdin_payload_to_real_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        monkeypatch: pytest.MonkeyPatch,
        env_name: str,
        backend_label: str,
    ) -> None:
        """Test validating STDIN data into a real cloud-backed target."""
        del backend_label
        target = real_remote_target_factory(env_name, suffix='validate-real')
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            ('validate', '--rules', rules_json, '--output', target.uri, '-'),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {target.uri}'
        assert File(target.uri, FileFormat.JSON).read() == sample_records

    def test_stdin_payload_to_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, object]],
        remote_storage_harness: RemoteStorageHarness,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating STDIN data and writing validated output to a remote URI."""
        target_uri = 's3://bucket/validate-output.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'validate',
                '--rules',
                rules_json,
                '--output',
                target_uri,
                '-',
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == sample_records
