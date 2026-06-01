"""
:mod:`tests.unit.telemetry.test_u_telemetry_config` module.

Unit tests for :mod:`etlplus.telemetry.config`.
"""

from __future__ import annotations

import etlplus.telemetry.config as telemetry_config_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestTelemetryConfig:
    """Unit tests for telemetry configuration resolution."""

    def test_exporter_parser_rejects_invalid_inputs(self) -> None:
        """Telemetry exporter parsing should reject invalid inputs predictably."""
        assert telemetry_config_mod._telemetry_exporter('bogus') is None

    def test_from_obj_parses_optional_mapping(self) -> None:
        """Telemetry config should parse supported fields tolerantly."""
        cfg = telemetry_config_mod.TelemetryConfig.from_obj(
            {
                'enabled': True,
                'exporter': 'opentelemetry',
                'service_name': 'etlplus-cli',
            },
        )

        assert cfg.enabled is True
        assert cfg.exporter == 'opentelemetry'
        assert cfg.service_name == 'etlplus-cli'

    def test_from_obj_returns_defaults_for_non_mapping_inputs(self) -> None:
        """Telemetry config should fall back to defaults for non-mapping input."""
        cfg = telemetry_config_mod.TelemetryConfig.from_obj(None)

        assert cfg == telemetry_config_mod.TelemetryConfig()

    def test_resolve_ignores_blank_service_name_overrides(self) -> None:
        """Blank service-name overrides should not hide configured names."""
        resolved = telemetry_config_mod.ResolvedTelemetryConfig.resolve(
            telemetry_config_mod.TelemetryConfig(service_name='configured-service'),
            env={'ETLPLUS_TELEMETRY_SERVICE_NAME': '   '},
            service_name=' ',
        )

        assert resolved.service_name == 'configured-service'

    def test_resolve_prefers_env_and_promotes_enabled_exporter(self) -> None:
        """Env values should override config and enabling telemetry picks OTel."""
        resolved = telemetry_config_mod.ResolvedTelemetryConfig.resolve(
            telemetry_config_mod.TelemetryConfig(enabled=False),
            env={
                'ETLPLUS_TELEMETRY_ENABLED': 'true',
                'ETLPLUS_TELEMETRY_SERVICE_NAME': 'env-service',
            },
        )

        assert resolved.enabled is True
        assert resolved.exporter == 'opentelemetry'
        assert resolved.service_name == 'env-service'
