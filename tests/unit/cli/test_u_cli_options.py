"""
:mod:`tests.unit.cli.test_u_cli_options` module.

Unit tests for :mod:`etlplus.cli._commands._options.helpers`.
"""

from __future__ import annotations

import pytest

import etlplus.cli._commands._options as cli_option_pkg
import etlplus.cli._commands._options.helpers as cli_options
import etlplus.cli._commands._options.init as init_options_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _assert_shared_option_metadata(
    kwargs: dict[str, object],
    *,
    help_text: str,
    metavar: str | None = None,
    show_default: bool | None = None,
) -> None:
    """Assert common Typer option metadata fields."""
    assert kwargs['help'] == help_text
    if metavar is None:
        assert 'metavar' not in kwargs
    else:
        assert kwargs['metavar'] == metavar
    if show_default is None:
        assert 'show_default' not in kwargs
    else:
        assert kwargs['show_default'] is show_default


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
    def test_connector_option_kwargs_context_specific_help(
        self,
        context: str,
        expected_help: str,
    ) -> None:
        """
        Test that :class:`Connector`-type helper preserve source and target
        wording.
        """
        kwargs = cli_options._typer_connector_option_kwargs(
            context=context,  # type: ignore[arg-type]
        )

        assert kwargs['metavar'] == 'CONNECTOR'
        assert kwargs['show_default'] is False
        assert kwargs['rich_help_panel'] == 'I/O overrides'
        assert expected_help in str(kwargs['help'])

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
        Test that :func:`_typer_flag_option_kwargs` includes only explicitly
        requested metadata.
        """
        assert (
            cli_options._typer_flag_option_kwargs(
                help_text,
                show_default=show_default,
            )
            == expected
        )

    def test_flag_option_kwargs_include_is_eager_when_requested(self) -> None:
        """
        Test that :func:`_typer_flag_option_kwargs` preserves Typer
        eager-evaluation metadata.
        """
        assert cli_options._typer_flag_option_kwargs(
            'Show the version and exit.',
            is_eager=True,
        ) == {
            'help': 'Show the version and exit.',
            'is_eager': True,
        }

    @pytest.mark.parametrize(
        ('context', 'expected_fragment'),
        [
            pytest.param(
                'source',
                'JSON payload, file path, URI/URL, or - for STDIN',
                id='source',
            ),
            pytest.param(
                'target',
                'file path, URI/URL, or - for STDOUT',
                id='target',
            ),
        ],
    )
    def test_resource_argument_kwargs_context_specific_help(
        self,
        context: str,
        expected_fragment: str,
    ) -> None:
        """Resource-argument helpers should preserve command semantics."""
        kwargs = cli_options._typer_resource_argument_kwargs(
            context=context,  # type: ignore[arg-type]
        )

        assert kwargs['metavar'] == context.upper()
        assert expected_fragment in str(kwargs['help'])

    @pytest.mark.parametrize(
        ('bound', 'expected_fragment'),
        [
            pytest.param('since', 'at or after', id='since'),
            pytest.param('until', 'at or before', id='until'),
        ],
    )
    def test_timestamp_option_kwargs_bound_specific_help(
        self,
        bound: str,
        expected_fragment: str,
    ) -> None:
        """Test that timestamp helpers preserve `since` and `until` wording."""
        kwargs = cli_options._typer_timestamp_option_kwargs(
            bound=bound,  # type: ignore[arg-type]
        )

        assert kwargs['metavar'] == 'ISO8601'
        assert kwargs['show_default'] is False
        assert expected_fragment in str(kwargs['help'])

    @pytest.mark.parametrize(
        ('context', 'expected_fragment'),
        [
            pytest.param('source', 'source is STDIN/inline', id='source'),
            pytest.param('target', 'target is STDIN/inline', id='target'),
        ],
    )
    def test_typer_format_option_kwargs_context_specific_help(
        self,
        context: str,
        expected_fragment: str,
    ) -> None:
        """Test that format helpers tailor help text by connector context."""
        kwargs = cli_options._typer_format_option_kwargs(
            context=context,  # type: ignore[arg-type]
        )

        assert kwargs['metavar'] == 'FORMAT'
        assert kwargs['show_default'] is False
        assert expected_fragment in str(kwargs['help'])

    @pytest.mark.parametrize(
        ('help_text', 'metavar', 'show_default'),
        [
            pytest.param(
                'Path to YAML-formatted configuration file.',
                'PATH',
                False,
                id='required-path',
            ),
            pytest.param(
                'Write rendered SQL to PATH (default: STDOUT).',
                'OUT',
                None,
                id='optional-show-default',
            ),
            pytest.param(
                'Name of the job to run',
                None,
                None,
                id='minimal',
            ),
        ],
    )
    def test_value_option_kwargs_preserve_metavar_and_optional_show_default(
        self,
        help_text: str,
        metavar: str | None,
        show_default: bool | None,
    ) -> None:
        """Test that scalar helpers preserve shared option metadata."""
        _assert_shared_option_metadata(
            cli_options._typer_value_option_kwargs(
                help_text,
                metavar=metavar,
                show_default=show_default,
            ),
            help_text=help_text,
            metavar=metavar,
            show_default=show_default,
        )


class TestOptionPackageExports:
    """Unit tests for init-option exports and re-exports."""

    def test_init_expected_aliases(self) -> None:
        """Test that the init option module exposes only the intended aliases."""
        assert init_options_mod.__all__ == [
            'InitDirectoryArgument',
            'InitForceOption',
        ]

    @pytest.mark.parametrize(
        ('name', 'expected'),
        [
            pytest.param(
                'InitDirectoryArgument',
                init_options_mod.InitDirectoryArgument,
                id='directory-argument',
            ),
            pytest.param(
                'InitForceOption',
                init_options_mod.InitForceOption,
                id='force-option',
            ),
        ],
    )
    def test_reexports_init_aliases(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that the CLI options package re-exports init command aliases."""
        assert name in cli_option_pkg.__all__
        assert getattr(cli_option_pkg, name) is expected
