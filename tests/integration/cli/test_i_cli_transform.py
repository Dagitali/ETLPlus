"""
:mod:`tests.integration.cli.test_i_cli_transform` module.

Integration-scope smoke tests for the ``etlplus transform`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from tests.integration.cli.pytest_cli_integration_support import assert_cli_success
from tests.integration.pytest_integration_support import REMOTE_STORAGE_ENV_CASES

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.integration.cli.pytest_cli_integration_support import (
        RealRemoteTargetFactory,
    )
    from tests.integration.pytest_integration_support import RemoteStorageHarness
    from tests.integration.pytest_integration_support import StdinText
    from tests.pytest_shared_support import CliInvoke
    from tests.pytest_shared_support import JsonOutputParser

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: HELPERS ========================================================== #


def _project_ids(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return the transform result expected from the shared select operation."""
    return [{'id': record['id']} for record in records]


# SECTION: TESTS ============================================================ #


class TestCliTransform:
    """Smoke tests for the ``etlplus transform`` CLI command."""

    def test_stdin_select(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        stdin_text: StdinText,
    ) -> None:
        """Test transforming a STDIN payload and projecting selected fields."""
        stdin_text(sample_records_json)
        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', '-'),
        )
        assert_cli_success(code, err)
        payload = parse_json_output(out)
        assert payload == _project_ids(sample_records)

    @pytest.mark.parametrize(
        'env_name',
        REMOTE_STORAGE_ENV_CASES,
    )
    def test_stdin_to_real_remote_file_target(
        self,
        cli_invoke: CliInvoke,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        stdin_text: StdinText,
        env_name: str,
    ) -> None:
        """Test transforming STDIN data into a real cloud-backed target."""
        target = real_remote_target_factory(env_name, suffix='transform-real')
        stdin_text(sample_records_json)

        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', target.uri),
        )

        assert_cli_success(code, err)
        assert out.strip() == f'Data transformed and saved to {target.uri}'
        assert File(target.uri, FileFormat.JSON).read() == _project_ids(
            sample_records,
        )

    def test_stdin_to_remote_file_target(
        self,
        cli_invoke: CliInvoke,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        remote_storage_harness: RemoteStorageHarness,
        stdin_text: StdinText,
    ) -> None:
        """Test transforming STDIN data and writing the result to a remote URI."""
        target_uri = 's3://bucket/transform-output.json'
        stdin_text(sample_records_json)

        code, out, err = cli_invoke(
            (
                'transform',
                '--operations',
                operations_json,
                '-',
                target_uri,
            ),
        )

        assert_cli_success(code, err)
        assert out.strip() == f'Data transformed and saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == _project_ids(
            sample_records,
        )
