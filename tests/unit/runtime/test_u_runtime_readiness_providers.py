"""
:mod:`tests.unit.runtime.test_u_runtime_readiness_providers` module.

Provider readiness unit tests for :mod:`etlplus.runtime.readiness._builder`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.runtime.readiness._builder as readiness_builder_mod
import etlplus.runtime.readiness._connectors as readiness_connectors_mod
import etlplus.runtime.readiness._providers as readiness_providers_mod
from etlplus.runtime.readiness._support import RequirementSpec

from .pytest_runtime_readiness import build_provider_check as _provider_check
from .pytest_runtime_readiness import build_provider_gap_row as _provider_gap
from .pytest_runtime_readiness import build_runtime_cfg as _cfg

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestReadinessReportBuilderProviders:
    """Provider readiness unit tests for :class:`ReadinessReportBuilder`."""

    @pytest.mark.parametrize(
        ('env', 'expected'),
        [
            pytest.param(
                {
                    'AWS_ACCESS_KEY_ID': 'access-key',
                    'AWS_SECRET_ACCESS_KEY': 'secret-key',
                },
                None,
                id='complete-explicit-pair',
            ),
            pytest.param(
                {},
                None,
                id='no-explicit-credentials',
            ),
            pytest.param(
                {'AWS_SECRET_ACCESS_KEY': 'secret-key'},
                {
                    'guidance': (
                        'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
                        'remove the partial explicit credential env vars and rely on '
                        'AWS_PROFILE, shared config files, container credentials, or '
                        'instance metadata.'
                    ),
                    'missing_env': ['AWS_ACCESS_KEY_ID'],
                    'provider': 'aws-s3',
                    'reason': (
                        'Incomplete explicit AWS access-key configuration was detected '
                        'for this S3 path.'
                    ),
                    'severity': 'error',
                },
                id='secret-only',
            ),
        ],
    )
    def test_explicit_aws_credential_gap_covers_guard_and_missing_access_key_paths(
        self,
        env: dict[str, str],
        expected: dict[str, object] | None,
    ) -> None:
        """Explicit AWS credential diagnostics should cover all short-circuit cases."""
        assert (
            readiness_providers_mod.ProviderEnvironmentPolicy
        ).explicit_aws_credential_gap(
            env,
        ) == expected

    @pytest.mark.parametrize(
        ('rows', 'status', 'message'),
        [
            pytest.param(
                [
                    _provider_gap(
                        connector='blob-source',
                        guidance=(
                            'Set AZURE_STORAGE_CONNECTION_STRING, set '
                            'AZURE_STORAGE_ACCOUNT_URL, or include the '
                            'account host in the path authority.'
                        ),
                        missing_env=[
                            'AZURE_STORAGE_CONNECTION_STRING',
                            'AZURE_STORAGE_ACCOUNT_URL',
                        ],
                        provider='azure-storage',
                        reason=(
                            'azure-blob path does not provide an account host '
                            'and no Azure storage bootstrap settings were '
                            'found.'
                        ),
                        role='source',
                        scheme='azure-blob',
                        severity='error',
                    ),
                ],
                'error',
                'Provider environment gaps: 1 error(s), 0 warning(s).',
                id='error-rows',
            ),
            pytest.param(
                [],
                'ok',
                'No provider-specific environment gaps were detected.',
                id='no-rows',
            ),
            pytest.param(
                [
                    _provider_gap(
                        connector='s3-source',
                        guidance=(
                            'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                            'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                            'files, container credentials, or instance '
                            'metadata.'
                        ),
                        missing_env=[
                            'AWS_ACCESS_KEY_ID',
                            'AWS_SECRET_ACCESS_KEY',
                            'AWS_SESSION_TOKEN',
                            'AWS_PROFILE',
                            'AWS_DEFAULT_PROFILE',
                            'AWS_ROLE_ARN',
                            'AWS_WEB_IDENTITY_TOKEN_FILE',
                            'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
                            'AWS_CONTAINER_CREDENTIALS_FULL_URI',
                            'AWS_SHARED_CREDENTIALS_FILE',
                            'AWS_CONFIG_FILE',
                        ],
                        provider='aws-s3',
                        reason=(
                            'No common AWS credential-chain environment hints '
                            'were detected for this S3 path.'
                        ),
                        role='source',
                        scheme='s3',
                        severity='warn',
                    ),
                ],
                'warn',
                'Provider environment warnings: 1.',
                id='warn-rows',
            ),
        ],
    )
    def test_provider_environment_checks_wrap_rows_by_severity(
        self,
        monkeypatch: pytest.MonkeyPatch,
        rows: list[dict[str, object]],
        status: str,
        message: str,
    ) -> None:
        """Provider check wrappers should map row severities into report rows."""
        monkeypatch.setattr(
            readiness_providers_mod.ProviderEnvironmentPolicy,
            'environment_rows',
            lambda cfg, env: rows,
        )

        checks = readiness_providers_mod.ProviderEnvironmentPolicy.environment_checks(
            cfg=cast(Any, _cfg()),
            env={},
            make_check=readiness_builder_mod.ReadinessReportBuilder.make_check,
            provider_environment_rows_fn=(
                readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows
            ),
        )

        assert checks == [
            _provider_check(message=message, rows=(rows or None), status=status),
        ]

    def test_provider_environment_rows_ignore_unhandled_connector_schemes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unhandled schemes should be ignored without producing provider rows."""
        monkeypatch.setattr(
            readiness_providers_mod,
            '_iter_connector_paths',
            lambda _cfg: [
                SimpleNamespace(
                    connector='local-source',
                    path='input.csv',
                    role='source',
                    scheme='file',
                ),
            ],
        )

        assert (
            readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows(
                cfg=cast(Any, _cfg()),
                env={},
            )
            == []
        )

    @pytest.mark.parametrize(
        ('cfg', 'env', 'expected'),
        [
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='blob-source',
                            format='csv',
                            path='azure-blob://container/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='blob-source',
                        guidance=(
                            'Set AZURE_STORAGE_CONNECTION_STRING, set '
                            'AZURE_STORAGE_ACCOUNT_URL, or include the '
                            'account host in the path authority.'
                        ),
                        missing_env=[
                            'AZURE_STORAGE_CONNECTION_STRING',
                            'AZURE_STORAGE_ACCOUNT_URL',
                        ],
                        provider='azure-storage',
                        reason=(
                            'azure-blob path does not provide an account host '
                            'and no Azure storage bootstrap settings were '
                            'found.'
                        ),
                        role='source',
                        scheme='azure-blob',
                        severity='error',
                    ),
                ],
                id='azure-bootstrap-error',
            ),
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='s3-source',
                            format='csv',
                            path='s3://bucket/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='s3-source',
                        guidance=(
                            'Set AWS_PROFILE or AWS_ACCESS_KEY_ID/'
                            'AWS_SECRET_ACCESS_KEY, or rely on shared config '
                            'files, container credentials, or instance '
                            'metadata.'
                        ),
                        missing_env=[
                            'AWS_ACCESS_KEY_ID',
                            'AWS_SECRET_ACCESS_KEY',
                            'AWS_SESSION_TOKEN',
                            'AWS_PROFILE',
                            'AWS_DEFAULT_PROFILE',
                            'AWS_ROLE_ARN',
                            'AWS_WEB_IDENTITY_TOKEN_FILE',
                            'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
                            'AWS_CONTAINER_CREDENTIALS_FULL_URI',
                            'AWS_SHARED_CREDENTIALS_FILE',
                            'AWS_CONFIG_FILE',
                        ],
                        provider='aws-s3',
                        reason=(
                            'No common AWS credential-chain environment hints '
                            'were detected for this S3 path.'
                        ),
                        role='source',
                        scheme='s3',
                        severity='warn',
                    ),
                ],
                id='s3-credential-warning',
            ),
            pytest.param(
                _cfg(
                    targets=[
                        SimpleNamespace(
                            connection_string=None,
                            dataset='warehouse',
                            name='warehouse_bigquery',
                            project='analytics-project',
                            provider='bigquery',
                            type='database',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='warehouse_bigquery',
                        guidance=(
                            'Set GOOGLE_APPLICATION_CREDENTIALS for an '
                            'explicit service-account credential, or rely on '
                            'gcloud Application Default Credentials, workload '
                            'identity, or instance metadata.'
                        ),
                        missing_env=[
                            'GOOGLE_APPLICATION_CREDENTIALS',
                            'GOOGLE_CLOUD_PROJECT',
                            'GCLOUD_PROJECT',
                            'CLOUDSDK_CONFIG',
                        ],
                        provider='gcp-bigquery',
                        reason=(
                            'No common Google Cloud credential-chain '
                            'environment hints were detected for this '
                            'BigQuery connector.'
                        ),
                        role='target',
                        scheme='bigquery',
                        severity='warn',
                    ),
                ],
                id='bigquery-credential-warning',
            ),
            pytest.param(
                _cfg(
                    targets=[
                        SimpleNamespace(
                            account='acme.us-east-1',
                            connection_string=None,
                            database='ANALYTICS',
                            name='warehouse_snowflake',
                            provider='snowflake',
                            schema='PUBLIC',
                            type='database',
                        ),
                    ],
                ),
                {},
                [
                    _provider_gap(
                        connector='warehouse_snowflake',
                        guidance=(
                            'Set SNOWFLAKE_USER plus SNOWFLAKE_PASSWORD or '
                            'SNOWFLAKE_PRIVATE_KEY_PATH, or rely on external '
                            'SSO or secret injection used by your runtime.'
                        ),
                        missing_env=[
                            'SNOWFLAKE_USER',
                            'SNOWFLAKE_PASSWORD',
                            'SNOWFLAKE_AUTHENTICATOR',
                            'SNOWFLAKE_PRIVATE_KEY_PATH',
                            'SNOWFLAKE_PRIVATE_KEY',
                        ],
                        provider='snowflake',
                        reason=(
                            'No common Snowflake credential environment hints '
                            'were detected for this connector.'
                        ),
                        role='target',
                        scheme='snowflake',
                        severity='warn',
                    ),
                ],
                id='snowflake-credential-warning',
            ),
        ],
    )
    def test_provider_environment_rows_report_expected_gaps(
        self,
        cfg: object,
        env: dict[str, str],
        expected: list[dict[str, object]],
    ) -> None:
        """Provider row generation should emit the expected Azure/S3 gaps."""
        rows = readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows(
            cfg=cast(Any, cfg),
            env=env,
        )

        assert rows == expected

    def test_provider_environment_rows_report_incomplete_explicit_aws_credentials(
        self,
    ) -> None:
        """Partial explicit AWS credentials should produce one provider error."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(
                    name='s3-source',
                    path='s3://bucket/input.csv',
                    type='file',
                ),
            ],
        )

        rows = readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows(
            cfg=cast(Any, cfg),
            env={'AWS_ACCESS_KEY_ID': 'access-key'},
        )

        assert rows == [
            {
                'connector': 's3-source',
                'guidance': (
                    'Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or '
                    'remove the partial explicit credential env vars and rely on '
                    'AWS_PROFILE, shared config files, container credentials, or '
                    'instance metadata.'
                ),
                'missing_env': ['AWS_SECRET_ACCESS_KEY'],
                'provider': 'aws-s3',
                'reason': (
                    'Incomplete explicit AWS access-key configuration was detected '
                    'for this S3 path.'
                ),
                'role': 'source',
                'scheme': 's3',
                'severity': 'error',
            },
        ]

    @pytest.mark.parametrize(
        ('cfg', 'env'),
        [
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='blob-source',
                            path='azure-blob://container/input.csv',
                            type='file',
                        ),
                    ],
                ),
                {
                    'AZURE_STORAGE_ACCOUNT_URL': (
                        'https://account.blob.core.windows.net'
                    ),
                    'AZURE_STORAGE_CREDENTIAL': 'secret',
                },
                id='explicit-azure-auth',
            ),
            pytest.param(
                _cfg(
                    sources=[
                        SimpleNamespace(
                            name='s3-source',
                            path='s3://bucket/input.csv',
                            type='file',
                        ),
                        SimpleNamespace(
                            name='local-source',
                            path='input.csv',
                            type='file',
                        ),
                    ],
                ),
                {'AWS_PROFILE': 'default'},
                id='s3-env-hint',
            ),
            pytest.param(
                _cfg(
                    targets=[
                        SimpleNamespace(
                            connection_string=None,
                            dataset='warehouse',
                            name='warehouse_bigquery',
                            project='analytics-project',
                            provider='bigquery',
                            type='database',
                        ),
                    ],
                ),
                {'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/etlplus-bigquery.json'},
                id='bigquery-env-hint',
            ),
            pytest.param(
                _cfg(
                    targets=[
                        SimpleNamespace(
                            account='acme.us-east-1',
                            connection_string=None,
                            database='ANALYTICS',
                            name='warehouse_snowflake',
                            provider='snowflake',
                            schema='PUBLIC',
                            type='database',
                        ),
                    ],
                ),
                {
                    'SNOWFLAKE_USER': 'etlplus',
                    'SNOWFLAKE_PASSWORD': 'secret',
                },
                id='snowflake-env-hint',
            ),
        ],
    )
    def test_provider_environment_rows_return_empty_when_auth_hints_exist(
        self,
        cfg: object,
        env: dict[str, str],
    ) -> None:
        """Provider rows should stay empty when explicit auth hints are present."""
        rows = readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows(
            cfg=cast(Any, cfg),
            env=env,
        )

        assert not rows

    def test_provider_environment_rows_skip_non_string_paths_and_warn_for_azure(
        self,
    ) -> None:
        """Test Azure warning branch and skipping connectors without string paths."""
        cfg = _cfg(
            sources=[
                SimpleNamespace(name='ignored', path=None, type='file'),
                SimpleNamespace(
                    name='blob-source',
                    path='azure-blob://container/input.csv',
                    type='file',
                ),
            ],
        )

        rows = readiness_providers_mod.ProviderEnvironmentPolicy.environment_rows(
            cfg=cast(Any, cfg),
            env={'AZURE_STORAGE_ACCOUNT_URL': 'https://account.blob.core.windows.net'},
        )

        assert rows == [
            {
                'connector': 'blob-source',
                'guidance': (
                    'Set AZURE_STORAGE_CREDENTIAL when the target is not public, '
                    'or use AZURE_STORAGE_CONNECTION_STRING for a fully explicit '
                    'configuration.'
                ),
                'missing_env': ['AZURE_STORAGE_CREDENTIAL'],
                'provider': 'azure-storage',
                'reason': (
                    'azure-blob access has no explicit Azure credential configured; '
                    'runtime access will only work for public resources or other '
                    'ambient authentication handled by the SDK call site.'
                ),
                'role': 'source',
                'scheme': 'azure-blob',
                'severity': 'warn',
            },
        ]

    def test_runtime_wrapper_helpers_delegate_to_extracted_helpers(
        self,
    ) -> None:
        """
        Thin readiness wrapper methods should preserve the extracted helper
        behavior.
        """
        requirement = RequirementSpec(
            ('boto3',),
            'boto3',
            'storage',
        )

        assert (
            readiness_providers_mod._aws_env_hint_present(
                {'AWS_PROFILE': 'default'},
            )
            is True
        )
        assert (
            readiness_providers_mod._azure_authority_has_account_host(
                'azure-blob://container@account.blob.core.windows.net/data.csv',
            )
            is True
        )
        assert readiness_connectors_mod.ConnectorReadinessPolicy.requirement_row(
            connector='out',
            detected_scheme='s3',
            reason='s3 storage path requires boto3',
            requirement=requirement,
            role='target',
        ) == {
            'connector': 'out',
            'detected_scheme': 's3',
            'extra': 'storage',
            'guidance': (
                'Install boto3 directly or install the ETLPlus "storage" extra. '
                'Required for "s3" storage paths.'
            ),
            'missing_package': 'boto3',
            'reason': 's3 storage path requires boto3',
            'role': 'target',
        }
