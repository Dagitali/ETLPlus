"""
:mod:`tests.unit.utils.test_u_utils_secrets` module.

Unit tests for :mod:`etlplus.utils._secrets`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etlplus.utils._secrets import DEFAULT_SECRETS_FILE_ENV_VAR
from etlplus.utils._secrets import SecretResolver

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='json_secrets_path')
def json_secrets_path_fixture(
    tmp_path: Path,
) -> Path:
    """Write one JSON secrets file used by file-backed resolver tests."""
    path = tmp_path / 'secrets.json'
    path.write_text(
        json.dumps({'service': {'password': 'json-secret'}}),
        encoding='utf-8',
    )
    return path


@pytest.fixture(name='yaml_secrets_path')
def yaml_secrets_path_fixture(
    tmp_path: Path,
) -> Path:
    """Write one YAML secrets file used by file-backed resolver tests."""
    path = tmp_path / 'secrets.yaml'
    path.write_text(
        'service:\n  password: yaml-secret\n  empty_value: ""\n',
        encoding='utf-8',
    )
    return path


# SECTION: TESTS ============================================================ #


class TestSecretResolver:
    """Unit tests for direct secret-token resolution."""

    @pytest.mark.parametrize(
        ('token_name', 'env_map', 'expected'),
        [
            pytest.param(
                'secret:API_TOKEN',
                {'API_TOKEN': 'env-secret'},
                'env-secret',
                id='implicit-env-provider',
            ),
            pytest.param(
                'secret:env:API_TOKEN',
                {'API_TOKEN': 'env-secret'},
                'env-secret',
                id='explicit-env-provider',
            ),
            pytest.param(
                'secret:MISSING',
                {'API_TOKEN': 'env-secret'},
                None,
                id='missing-env-key',
            ),
            pytest.param(
                'plain:API_TOKEN',
                {'API_TOKEN': 'env-secret'},
                None,
                id='non-secret-token',
            ),
            pytest.param(
                'secret:unsupported:key',
                {'API_TOKEN': 'env-secret'},
                None,
                id='unsupported-provider',
            ),
        ],
    )
    def test_resolve_token_handles_env_and_invalid_inputs(
        self,
        token_name: str,
        env_map: dict[str, str],
        expected: object | None,
    ) -> None:
        """Test direct token resolution for env-backed and invalid tokens."""
        assert SecretResolver(env_map).resolve_token(token_name) == expected

    @pytest.mark.parametrize(
        ('token_name', 'env_map'),
        [
            pytest.param('secret:', None, id='empty-implicit-env-key'),
            pytest.param(
                'secret:env:', {'API_TOKEN': 'env-secret'}, id='empty-explicit-env-key',
            ),
        ],
    )
    def test_resolve_token_returns_none_for_empty_env_secret_keys(
        self,
        token_name: str,
        env_map: dict[str, str] | None,
    ) -> None:
        """Test env-backed resolution rejects empty secret-key references."""
        assert SecretResolver(env_map).resolve_token(token_name) is None

    @pytest.mark.parametrize(
        ('key', 'env_map', 'expected'),
        [
            pytest.param('', {'API_TOKEN': 'env-secret'}, None, id='empty-env-key'),
            pytest.param(
                'service.password', {}, None, id='missing-secrets-file-env-var',
            ),
            pytest.param(
                'service.password',
                {DEFAULT_SECRETS_FILE_ENV_VAR: 123},
                None,
                id='non-string-secrets-file-path',
            ),
        ],
    )
    def test_resolve_token_handles_missing_file_configuration(
        self,
        key: str,
        env_map: dict[str, object],
        expected: object | None,
    ) -> None:
        """Test file-backed resolution fails closed when config is incomplete."""
        assert SecretResolver(env_map).resolve_token(f'secret:file:{key}') == expected

    @pytest.mark.parametrize(
        ('extension', 'key', 'expected'),
        [
            pytest.param('json', 'service.password', 'json-secret', id='json-file'),
            pytest.param('yaml', 'service.password', 'yaml-secret', id='yaml-file'),
            pytest.param('yaml', 'service.missing', None, id='missing-key'),
            pytest.param('yaml', 'service.empty_value', None, id='empty-leaf-value'),
        ],
    )
    def test_resolve_token_reads_local_mapping_files(
        self,
        extension: str,
        key: str,
        expected: object | None,
        json_secrets_path: Path,
        yaml_secrets_path: Path,
    ) -> None:
        """Test file-backed secret resolution for JSON and YAML mappings."""
        secrets_path = json_secrets_path if extension == 'json' else yaml_secrets_path
        env_map = {DEFAULT_SECRETS_FILE_ENV_VAR: str(secrets_path)}

        assert SecretResolver(env_map).resolve_token(f'secret:file:{key}') == expected

    def test_resolve_token_ignores_non_mapping_file_payloads(
        self,
        tmp_path: Path,
    ) -> None:
        """Test file-backed resolution ignores scalar payload files."""
        path = tmp_path / 'scalar.yaml'
        path.write_text('value\n', encoding='utf-8')

        assert (
            SecretResolver(
                {DEFAULT_SECRETS_FILE_ENV_VAR: str(path)},
            ).resolve_token('secret:file:service.password')
            is None
        )

    def test_resolve_token_ignores_missing_local_file(self, tmp_path: Path) -> None:
        """Test file-backed resolution returns ``None`` for missing files."""
        missing = tmp_path / 'missing.json'

        assert (
            SecretResolver(
                {DEFAULT_SECRETS_FILE_ENV_VAR: str(missing)},
            ).resolve_token('secret:file:service.password')
            is None
        )

    def test_secrets_file_path_expands_user_home(self) -> None:
        """Test configured secret file paths expand user-home markers."""
        resolver = SecretResolver({DEFAULT_SECRETS_FILE_ENV_VAR: '~/secrets.json'})

        assert resolver._secrets_file_path() == Path('~/secrets.json').expanduser()

    @pytest.mark.parametrize(
        'env_map',
        [
            pytest.param(None, id='missing-env-map'),
            pytest.param({}, id='missing-path'),
            pytest.param({DEFAULT_SECRETS_FILE_ENV_VAR: ''}, id='empty-path'),
        ],
    )
    def test_secrets_file_path_returns_none_without_usable_path(
        self,
        env_map: dict[str, str] | None,
    ) -> None:
        """Test configured secret file paths require a non-empty string."""
        assert SecretResolver(env_map)._secrets_file_path() is None
