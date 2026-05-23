"""
:mod:`tests.integration.cli.test_i_cli_transform` module.

Integration-scope smoke tests for the ``etlplus transform`` CLI command.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from tests.integration.conftest import REMOTE_STORAGE_ENV_CASES
from tests.integration.conftest import REMOTE_STORAGE_ENV_IDS

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RealRemoteTargetFactory
    from tests.integration.conftest import RemoteStorageHarness

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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test transforming a STDIN payload and projecting selected fields."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', '-'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == _project_ids(sample_records)

    @pytest.mark.parametrize(
        'env_name',
        REMOTE_STORAGE_ENV_CASES,
        ids=REMOTE_STORAGE_ENV_IDS,
    )
    def test_stdin_to_real_remote_file_target(
        self,
        cli_invoke: CliInvoke,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        real_remote_target_factory: RealRemoteTargetFactory,
        monkeypatch: pytest.MonkeyPatch,
        env_name: str,
    ) -> None:
        """Test transforming STDIN data into a real cloud-backed target."""
        target = real_remote_target_factory(env_name, suffix='transform-real')
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', target.uri),
        )

        assert code == 0
        assert err.strip() == ''
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test transforming STDIN data and writing the result to a remote URI."""
        target_uri = 's3://bucket/transform-output.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'transform',
                '--operations',
                operations_json,
                '-',
                target_uri,
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'Data transformed and saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == _project_ids(
            sample_records,
        )
