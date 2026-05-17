"""
:mod:`etlplus.runtime.readiness._support` module.

Internal shared types and constants for runtime readiness checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Final
from typing import Literal

from ...utils._imports import ImportRequirement as RequirementSpec
from ...utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'AWS_ENV_HINTS',
    'AZURE_ENV_HINTS',
    'AZURE_STORAGE_BOOTSTRAP_ENV',
    'AZURE_STORAGE_CREDENTIAL_ENV',
    'DATABASE_PROVIDER_EXTRA_REQUIREMENTS',
    'FORMAT_EXTRA_REQUIREMENTS',
    'GCP_ENV_HINTS',
    'QUEUE_SERVICE_EXTRA_REQUIREMENTS',
    'SCHEME_EXTRA_REQUIREMENTS',
    'SNOWFLAKE_ENV_HINTS',
    'SUPPORTED_PYTHON_RANGE',
    # Classes
    'RequirementSpec',
    'ResolvedConfigContext',
    # Type Aliases
    'ReadinessRow',
]


# SECTION: TYPE ALIASES ===================================================== #


type CheckStatus = Literal['ok', 'warn', 'error', 'skipped']
type ReadinessRow = dict[str, Any]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class ResolvedConfigContext:
    """Resolved config state reused across config readiness checks."""

    # -- Instance Methods -- #

    raw: StrAnyMap
    effective_env: dict[str, str]
    unresolved_tokens: list[str]
    resolved_raw: StrAnyMap
    resolved_cfg: object | None


# SECTION: CONSTANTS ======================================================== #


AZURE_STORAGE_CREDENTIAL_ENV: Final[str] = 'AZURE_STORAGE_CREDENTIAL'

# TODO: Conform constants for provider hints to a more systematic structure
# TODO: with provider-specific sections and guidance for additional services
# TODO: (for example, GCP and Databricks).

AWS_ENV_HINTS: Final[tuple[str, ...]] = (
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
)
AZURE_STORAGE_BOOTSTRAP_ENV: Final[tuple[str, ...]] = (
    'AZURE_STORAGE_CONNECTION_STRING',
    'AZURE_STORAGE_ACCOUNT_URL',
)
AZURE_ENV_HINTS: Final[tuple[str, ...]] = (
    *AZURE_STORAGE_BOOTSTRAP_ENV,
    AZURE_STORAGE_CREDENTIAL_ENV,
)
GCP_ENV_HINTS: Final[tuple[str, ...]] = (
    'GOOGLE_APPLICATION_CREDENTIALS',
    'GOOGLE_CLOUD_PROJECT',
    'GCLOUD_PROJECT',
    'CLOUDSDK_CONFIG',
)
SNOWFLAKE_ENV_HINTS: Final[tuple[str, ...]] = (
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_AUTHENTICATOR',
    'SNOWFLAKE_PRIVATE_KEY_PATH',
    'SNOWFLAKE_PRIVATE_KEY',
)

DATABASE_PROVIDER_EXTRA_REQUIREMENTS: Final[dict[str, RequirementSpec]] = {
    'bigquery': RequirementSpec(
        ('google.cloud.bigquery', 'sqlalchemy_bigquery'),
        'google-cloud-bigquery/sqlalchemy-bigquery',
        'database-bigquery',
    ),
    'snowflake': RequirementSpec(
        ('snowflake.connector', 'snowflake.sqlalchemy'),
        'snowflake-connector-python/snowflake-sqlalchemy',
        'database-snowflake',
    ),
}

FORMAT_EXTRA_REQUIREMENTS: Final[dict[str, RequirementSpec]] = {
    'dta': RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'hdf5': RequirementSpec(('tables',), 'tables'),
    'rda': RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'rds': RequirementSpec(('pyreadr',), 'pyreadr', 'file'),
    'sav': RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
    'zsav': RequirementSpec(('pyreadstat',), 'pyreadstat', 'file'),
}
QUEUE_SERVICE_EXTRA_REQUIREMENTS: Final[dict[str, RequirementSpec]] = {
    'amqp': RequirementSpec(('pika',), 'pika', 'queue-amqp'),
    'azure-service-bus': RequirementSpec(
        ('azure.servicebus',),
        'azure-servicebus',
        'queue-azure',
    ),
    'gcp-pubsub': RequirementSpec(
        ('google.cloud.pubsub',),
        'google-cloud-pubsub',
        'queue-gcp',
    ),
    'redis': RequirementSpec(('redis',), 'redis', 'queue-redis'),
    'aws-sqs': RequirementSpec(('boto3',), 'boto3', 'queue-aws'),
}
SCHEME_EXTRA_REQUIREMENTS: Final[dict[str, RequirementSpec]] = {
    'abfs': RequirementSpec(
        ('azure.storage.filedatalake',),
        'azure-storage-file-datalake',
        'storage',
    ),
    'azure-blob': RequirementSpec(
        ('azure.storage.blob',),
        'azure-storage-blob',
        'storage',
    ),
    's3': RequirementSpec(('boto3',), 'boto3', 'storage'),
}

SUPPORTED_PYTHON_RANGE: Final[tuple[tuple[int, int], tuple[int, int]]] = (
    (3, 13),
    (3, 15),
)
