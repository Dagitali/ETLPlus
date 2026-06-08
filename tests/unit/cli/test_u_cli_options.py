"""
:mod:`tests.unit.cli.test_u_cli_options` module.

Unit tests for :mod:`etlplus.cli._commands._options.helpers`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import get_args

import pytest
import typer

import etlplus.cli._commands._options as cli_option_pkg
import etlplus.cli._commands._options.helpers as cli_options
import etlplus.cli._commands._options.init as init_options_mod
from tests.unit.pytest_export_contracts import assert_package_exports
from tests.unit.pytest_export_contracts import export_names

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: CONSTANTS ======================================================== #


HELPER_OPTION_EXPORTS: tuple[tuple[str, object], ...] = (
    ('typer_connector_option_alias', cli_options.typer_connector_option_alias),
    ('typer_flag_option_alias', cli_options.typer_flag_option_alias),
    ('typer_flag_option_kwargs', cli_options.typer_flag_option_kwargs),
    ('typer_format_option_alias', cli_options.typer_format_option_alias),
    ('typer_option_alias', cli_options.typer_option_alias),
    ('typer_resource_argument_alias', cli_options.typer_resource_argument_alias),
    ('typer_timestamp_option_alias', cli_options.typer_timestamp_option_alias),
    ('typer_value_option_alias', cli_options.typer_value_option_alias),
)
INIT_OPTION_EXPORTS: tuple[tuple[str, object], ...] = (
    ('InitDirectoryArgument', init_options_mod.InitDirectoryArgument),
    ('InitForceOption', init_options_mod.InitForceOption),
)
OPTION_ALIAS_CASES = (
    pytest.param(
        cli_options.typer_connector_option_alias,
        str,
        '--source-type',
        {'context': 'source'},
        'CONNECTOR',
        'Override the inferred source type',
        'I/O overrides',
        id='connector-source',
    ),
    pytest.param(
        cli_options.typer_connector_option_alias,
        str,
        '--target-type',
        {'context': 'target'},
        'CONNECTOR',
        'Override the inferred target type',
        'I/O overrides',
        id='connector-target',
    ),
    pytest.param(
        cli_options.typer_format_option_alias,
        str | None,
        '--source-format',
        {'context': 'source'},
        'FORMAT',
        'source is STDIN/inline',
        None,
        id='format-source',
    ),
    pytest.param(
        cli_options.typer_format_option_alias,
        str | None,
        '--target-format',
        {'context': 'target'},
        'FORMAT',
        'target is STDIN/inline',
        None,
        id='format-target',
    ),
    pytest.param(
        cli_options.typer_timestamp_option_alias,
        str | None,
        '--since',
        {'bound': 'since'},
        'ISO8601',
        'at or after',
        None,
        id='timestamp-since',
    ),
    pytest.param(
        cli_options.typer_timestamp_option_alias,
        str | None,
        '--until',
        {'bound': 'until'},
        'ISO8601',
        'at or before',
        None,
        id='timestamp-until',
    ),
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _option_alias_metadata(
    alias: object,
) -> tuple[object, typer.models.OptionInfo]:
    """Return the value type and Typer option metadata from one option alias."""
    value_type, option_info = get_args(alias)
    assert isinstance(option_info, typer.models.OptionInfo)
    return value_type, option_info


# SECTION: TESTS ============================================================ #


class TestHelperOptionKwargs:
    """Unit tests for shared Typer option helper functions."""

    @pytest.mark.parametrize(
        ('help_text', 'kwargs', 'expected'),
        [
            pytest.param(
                'List data sources',
                {'show_default': None},
                {'help': 'List data sources'},
                id='implicit-show-default',
            ),
            pytest.param(
                'Show the version and exit.',
                {'show_default': False},
                {'help': 'Show the version and exit.', 'show_default': False},
                id='explicit-show-default',
            ),
            pytest.param(
                'Show the version and exit.',
                {'is_eager': True},
                {'help': 'Show the version and exit.', 'is_eager': True},
                id='eager',
            ),
        ],
    )
    def test_flag_option_kwargs_include_requested_metadata(
        self,
        help_text: str,
        kwargs: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that :func:`typer_flag_option_kwargs` includes only explicitly
        requested metadata.
        """
        assert (
            cli_options.typer_flag_option_kwargs(
                help_text,
                **kwargs,
            )
            == expected
        )

    def test_helper_module_exports_intended_public_api(self) -> None:
        """Option helper exports should reflect the public helper surface."""
        assert_package_exports(
            package_module=cli_options,
            expected_exports=HELPER_OPTION_EXPORTS,
        )

    @pytest.mark.parametrize(
        (
            'alias_factory',
            'value_type',
            'param_decl',
            'kwargs',
            'expected_metavar',
            'expected_help',
            'expected_panel',
        ),
        OPTION_ALIAS_CASES,
    )
    def test_option_aliases_preserve_context_specific_metadata(
        self,
        alias_factory: Callable[..., object],
        value_type: object,
        param_decl: str,
        kwargs: dict[str, object],
        expected_metavar: str,
        expected_help: str,
        expected_panel: str | None,
    ) -> None:
        """
        Test that context-specific option aliases preserve Typer metadata.
        """
        _, option_info = _option_alias_metadata(
            alias_factory(value_type, param_decl, **kwargs),
        )

        assert option_info.metavar == expected_metavar
        assert option_info.show_default is False
        assert expected_help in str(option_info.help)
        if expected_panel is not None:
            assert option_info.rich_help_panel == expected_panel

    def test_resource_argument_alias_builds_typer_argument_metadata(self) -> None:
        """Resource argument aliases should wrap one Typer argument metadata object."""
        alias = cli_options.typer_resource_argument_alias(
            str,
            'SOURCE',
            context='source',
        )

        value_type, argument_info = get_args(alias)
        assert value_type is str
        assert isinstance(argument_info, typer.models.ArgumentInfo)
        assert 'Extract data from SOURCE' in str(argument_info.help)

    def test_value_option_alias_builds_typer_option_metadata(self) -> None:
        """Scalar option aliases should carry the requested metadata."""
        alias = cli_options.typer_value_option_alias(
            str,
            '--job',
            help_text='Name of the job to run',
            metavar='JOB',
            show_default=None,
        )

        value_type, option_info = _option_alias_metadata(alias)
        assert value_type is str
        assert option_info.help == 'Name of the job to run'
        assert option_info.metavar == 'JOB'


class TestOptionPackageExports:
    """Unit tests for init-option exports and re-exports."""

    def test_init_expected_aliases(self) -> None:
        """Test that the init option module exposes only the intended aliases."""
        assert_package_exports(
            package_module=init_options_mod,
            expected_exports=INIT_OPTION_EXPORTS,
        )

    @pytest.mark.parametrize(
        ('name', 'expected'),
        INIT_OPTION_EXPORTS,
        ids=export_names(INIT_OPTION_EXPORTS),
    )
    def test_reexports_init_aliases(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that the CLI options package re-exports init command aliases."""
        assert name in cli_option_pkg.__all__
        assert getattr(cli_option_pkg, name) is expected
