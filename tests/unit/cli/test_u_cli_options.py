"""
:mod:`tests.unit.cli.test_u_cli_options` module.

Unit tests for :mod:`etlplus.cli._commands._options.helpers`.
"""

from __future__ import annotations

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


# SECTION: TESTS ============================================================ #


class TestHelperOptionKwargs:
    """Unit tests for shared Typer option helper functions."""

    @pytest.mark.parametrize(
        ('context', 'expected_help'),
        [
            pytest.param('source', 'Override the inferred source type', id='source'),
            pytest.param('target', 'Override the inferred target type', id='target'),
        ],
    )
    def test_connector_option_alias_context_specific_help(
        self,
        context: str,
        expected_help: str,
    ) -> None:
        """
        Test that :class:`Connector`-type helper preserve source and target
        wording.
        """
        _, option_info = get_args(
            cli_options.typer_connector_option_alias(
                str,
                f'--{context}-type',
                context=context,  # type: ignore[arg-type]
            ),
        )

        assert isinstance(option_info, typer.models.OptionInfo)
        assert option_info.metavar == 'CONNECTOR'
        assert option_info.show_default is False
        assert option_info.rich_help_panel == 'I/O overrides'
        assert expected_help in str(option_info.help)

    @pytest.mark.parametrize(
        ('help_text', 'show_default', 'expected'),
        [
            pytest.param(
                'List data sources',
                None,
                {'help': 'List data sources'},
                id='implicit-show-default',
            ),
            pytest.param(
                'Show the version and exit.',
                False,
                {'help': 'Show the version and exit.', 'show_default': False},
                id='explicit-show-default',
            ),
        ],
    )
    def test_flag_option_kwargs_include_requested_metadata(
        self,
        help_text: str,
        show_default: bool | None,
        expected: dict[str, object],
    ) -> None:
        """
        Test that :func:`typer_flag_option_kwargs` includes only explicitly
        requested metadata.
        """
        assert (
            cli_options.typer_flag_option_kwargs(
                help_text,
                show_default=show_default,
            )
            == expected
        )

    def test_flag_option_kwargs_include_is_eager_when_requested(self) -> None:
        """
        Test that :func:`typer_flag_option_kwargs` preserves Typer
        eager-evaluation metadata.
        """
        assert cli_options.typer_flag_option_kwargs(
            'Show the version and exit.',
            is_eager=True,
        ) == {
            'help': 'Show the version and exit.',
            'is_eager': True,
        }

    def test_helper_module_exports_intended_public_api(self) -> None:
        """Option helper exports should reflect the public helper surface."""
        assert_package_exports(
            package_module=cli_options,
            expected_exports=HELPER_OPTION_EXPORTS,
        )

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

    @pytest.mark.parametrize(
        ('bound', 'expected_fragment'),
        [
            pytest.param('since', 'at or after', id='since'),
            pytest.param('until', 'at or before', id='until'),
        ],
    )
    def test_timestamp_option_alias_bound_specific_help(
        self,
        bound: str,
        expected_fragment: str,
    ) -> None:
        """Test that timestamp helpers preserve `since` and `until` wording."""
        _, option_info = get_args(
            cli_options.typer_timestamp_option_alias(
                str | None,
                f'--{bound}',
                bound=bound,  # type: ignore[arg-type]
            ),
        )

        assert isinstance(option_info, typer.models.OptionInfo)
        assert option_info.metavar == 'ISO8601'
        assert option_info.show_default is False
        assert expected_fragment in str(option_info.help)

    @pytest.mark.parametrize(
        ('context', 'expected_fragment'),
        [
            pytest.param('source', 'source is STDIN/inline', id='source'),
            pytest.param('target', 'target is STDIN/inline', id='target'),
        ],
    )
    def test_typer_format_option_alias_context_specific_help(
        self,
        context: str,
        expected_fragment: str,
    ) -> None:
        """Test that format helpers tailor help text by connector context."""
        _, option_info = get_args(
            cli_options.typer_format_option_alias(
                str | None,
                f'--{context}-format',
                context=context,  # type: ignore[arg-type]
            ),
        )

        assert isinstance(option_info, typer.models.OptionInfo)
        assert option_info.metavar == 'FORMAT'
        assert option_info.show_default is False
        assert expected_fragment in str(option_info.help)

    def test_value_option_alias_builds_typer_option_metadata(self) -> None:
        """Scalar option aliases should carry the requested metadata."""
        alias = cli_options.typer_value_option_alias(
            str,
            '--job',
            help_text='Name of the job to run',
            metavar='JOB',
            show_default=None,
        )

        value_type, option_info = get_args(alias)
        assert value_type is str
        assert isinstance(option_info, typer.models.OptionInfo)
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
