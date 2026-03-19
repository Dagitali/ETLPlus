"""
:mod:`tests.integration.cli.test_i_cli_extract` module.

Integration-scope smoke tests for the ``etlplus extract`` CLI command.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteSourceFactory
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliExtract:
    """Smoke tests for the ``etlplus extract`` CLI command."""

    def test_extract_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        json_payload_file: Path,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """
        Test extracting JSON from a file and emitting the matching payload.
        """
        code, out, err = cli_invoke(('extract', str(json_payload_file)))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == sample_records

    @pytest.mark.parametrize(
        ('env_name', 'backend_label'),
        [
            ('ETLPLUS_TEST_S3_URI', 's3'),
            ('ETLPLUS_TEST_AZURE_BLOB_URI', 'azure-blob'),
        ],
        ids=['s3', 'azure-blob'],
    )
    def test_extract_real_remote_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records: list[dict[str, Any]],
        real_remote_source_factory: RealRemoteSourceFactory,
        env_name: str,
        backend_label: str,
    ) -> None:
        """Test extracting JSON from a real cloud-backed remote URI."""
        del backend_label
        source = real_remote_source_factory(
            env_name,
            payload=sample_records,
            suffix='extract-real',
            file_format='json',
        )

        code, out, err = cli_invoke(('extract', source.uri))

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == sample_records

    def test_extract_remote_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        remote_storage_harness: RemoteStorageHarness,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test extracting JSON from a remote URI via the CLI."""
        remote_storage_harness.set_json(
            's3://bucket/input.json',
            sample_records,
        )

        code, out, err = cli_invoke(('extract', 's3://bucket/input.json'))

        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == sample_records
