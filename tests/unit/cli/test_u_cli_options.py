"""
:mod:`tests.unit.cli.test_u_cli_options` module.

Unit tests for :mod:`etlplus.cli._options`.
"""

from __future__ import annotations

import pytest

import etlplus.cli._options as cli_options

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('context', 'expected_help'),
    [
        ('source', 'Override the inferred source type'),
        ('target', 'Override the inferred target type'),
    ],
)
def test_connector_option_kwargs_context_specific_help(
    context: str,
    expected_help: str,
) -> None:
    """Test that connector-type helper preserves source/target wording."""
    kwargs = cli_options._typer_connector_option_kwargs(
        context=context,  # type: ignore[arg-type]
    )
    assert kwargs['metavar'] == 'CONNECTOR'
    assert kwargs['show_default'] is False
    assert kwargs['rich_help_panel'] == 'I/O overrides'
    assert expected_help in str(kwargs['help'])


@pytest.mark.parametrize(
    ('context', 'expected_fragment'),
    [
        ('source', 'JSON payload, file path, URI/URL, or - for STDIN'),
        ('target', 'file path, URI/URL, or - for STDOUT'),
    ],
)
def test_resource_argument_kwargs_context_specific_help(
    context: str,
    expected_fragment: str,
) -> None:
    """Test that resource-argument helper preserves command semantics."""
    kwargs = cli_options._typer_resource_argument_kwargs(
        context=context,  # type: ignore[arg-type]
    )
    assert kwargs['metavar'] == context.upper()
    assert expected_fragment in str(kwargs['help'])


@pytest.mark.parametrize(
    ('bound', 'expected_fragment'),
    [
        ('since', 'at or after'),
        ('until', 'at or before'),
    ],
)
def test_timestamp_option_kwargs_bound_specific_help(
    bound: str,
    expected_fragment: str,
) -> None:
    """Test that timestamp helper preserves since/until wording."""
    kwargs = cli_options._typer_timestamp_option_kwargs(
        bound=bound,  # type: ignore[arg-type]
    )
    assert kwargs['metavar'] == 'ISO8601'
    assert kwargs['show_default'] is False
    assert expected_fragment in str(kwargs['help'])


@pytest.mark.parametrize(
    ('context', 'expected_fragment'),
    [
        ('source', 'source is STDIN/inline'),
        ('target', 'target is STDIN/inline'),
    ],
)
def test_typer_format_option_kwargs_context_specific_help(
    context: str,
    expected_fragment: str,
) -> None:
    """Test that the option helper tailors help text by connector context."""
    kwargs = cli_options.typer_format_option_kwargs(
        context=context,  # type: ignore[arg-type]
    )
    assert kwargs['metavar'] == 'FORMAT'
    assert kwargs['show_default'] is False
    assert expected_fragment in str(kwargs['help'])
