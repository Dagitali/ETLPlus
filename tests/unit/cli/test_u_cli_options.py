"""
:mod:`tests.unit.cli.test_u_cli_options` module.

Unit tests for :mod:`etlplus.cli.options`.
"""

from __future__ import annotations

import pytest

from etlplus.cli.options import typer_format_option_kwargs

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


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
    """Option helper should tailor help text by connector context."""
    kwargs = typer_format_option_kwargs(
        context=context,  # type: ignore[arg-type]
    )
    assert kwargs['metavar'] == 'FORMAT'
    assert kwargs['show_default'] is False
    assert expected_fragment in str(kwargs['help'])
