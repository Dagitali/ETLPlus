"""
:mod:`etlplus.telemetry.config` module.

Telemetry configuration models shared by config parsing and runtime export.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal
from typing import Self
from typing import cast

from ..utils import MappingParser
from ..utils import TextNormalizer
from ..utils import ValueParser
from ..utils._types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ResolvedTelemetryConfig',
    'TelemetryConfig',
]


# SECTION: TYPE ALIASES ===================================================== #


type TelemetryExporter = Literal['none', 'opentelemetry']


# SECTION: INTERNAL CONSTANTS =============================================== #


_VALID_EXPORTERS = frozenset({'none', 'opentelemetry'})


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _telemetry_exporter(
    value: object,
) -> TelemetryExporter | None:
    """Return one supported telemetry exporter name when valid."""
    if not isinstance(value, str):
        return None
    normalized = TextNormalizer.normalize(value)
    if normalized in _VALID_EXPORTERS:
        return cast(TelemetryExporter, normalized)
    return None


# SECTION: DATA CLASSES ===================================================== #


@dataclass(kw_only=True, slots=True, frozen=True)
class TelemetryConfig:
    """Pipeline-level telemetry defaults."""

    # -- Instance Attributes -- #

    enabled: bool = False
    exporter: TelemetryExporter | None = None
    service_name: str | None = None

    # -- Class Methods -- #

    @classmethod
    def from_obj(
        cls,
        obj: StrAnyMap | None,
    ) -> Self:
        """Parse one optional telemetry config mapping."""
        if not (data := MappingParser.optional(obj)):
            return cls()

        raw_service_name = data.get('service_name')
        return cls(
            enabled=ValueParser.bool_flag(data.get('enabled'), default=False),
            exporter=_telemetry_exporter(data.get('exporter')),
            service_name=(
                raw_service_name.strip()
                if isinstance(raw_service_name, str) and raw_service_name.strip()
                else None
            ),
        )


@dataclass(kw_only=True, slots=True, frozen=True)
class ResolvedTelemetryConfig:
    """Resolved telemetry settings used at runtime."""

    enabled: bool
    exporter: TelemetryExporter
    service_name: str

    @classmethod
    def resolve(
        cls,
        config: TelemetryConfig | None,
        *,
        env: Mapping[str, str] | None = None,
        enabled: bool | None = None,
        exporter: str | None = None,
        service_name: str | None = None,
    ) -> Self:
        """
        Return effective telemetry settings from config and environment.

        This method considers the provided config, environment variables, and
        explicit overrides to determine the final telemetry settings.
        """
        telemetry_cfg = config if config is not None else TelemetryConfig()
        env_map = env or {}

        resolved_exporter = (
            _telemetry_exporter(exporter)
            or _telemetry_exporter(env_map.get('ETLPLUS_TELEMETRY_EXPORTER'))
            or telemetry_cfg.exporter
            or 'none'
        )
        resolved_enabled = (
            enabled
            if enabled is not None
            else ValueParser.bool_flag(
                env_map.get('ETLPLUS_TELEMETRY_ENABLED'),
                default=(telemetry_cfg.enabled or resolved_exporter != 'none'),
            )
        )
        if resolved_enabled and resolved_exporter == 'none':
            resolved_exporter = 'opentelemetry'

        raw_service_name = (
            service_name
            or env_map.get('ETLPLUS_TELEMETRY_SERVICE_NAME')
            or telemetry_cfg.service_name
            or 'etlplus'
        )
        cleaned_service_name = raw_service_name.strip() or 'etlplus'

        return cls(
            enabled=resolved_enabled,
            exporter=resolved_exporter,
            service_name=cleaned_service_name,
        )
