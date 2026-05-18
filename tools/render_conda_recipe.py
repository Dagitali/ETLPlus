"""Render the draft conda-forge recipe template for local validation."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _parser() -> argparse.ArgumentParser:
    """Return the command-line parser for recipe rendering."""
    parser = argparse.ArgumentParser(
        description='Render packaging/conda/meta.yaml.j2 with release values.',
    )
    parser.add_argument(
        '--template',
        default='packaging/conda/meta.yaml.j2',
        help='Path to the conda recipe template.',
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path where rendered meta.yaml should be written.',
    )
    parser.add_argument(
        '--version',
        required=True,
        help='Release version to substitute for <release-version>.',
    )
    parser.add_argument(
        '--sha256',
        required=True,
        help='PyPI sdist SHA256 to substitute for <sdist-sha256>.',
    )
    parser.add_argument(
        '--maintainer',
        required=True,
        help='GitHub maintainer handle to substitute for <maintainer-github-handle>.',
    )
    parser.add_argument(
        '--source-path',
        help=(
            'Optional local source path for validation builds. When provided, '
            'the rendered recipe uses source: path instead of the PyPI sdist URL.'
        ),
    )
    return parser


# SECTION: FUNCTIONS ======================================================== #


def render_recipe(
    *,
    template_path: Path,
    output_path: Path,
    version: str,
    sha256: str,
    maintainer: str,
    source_path: Path | None = None,
) -> None:
    """Render one conda recipe template with release-specific values."""
    text = template_path.read_text(encoding='utf-8')
    rendered = (
        text.replace('<release-version>', version)
        .replace('<sdist-sha256>', sha256)
        .replace('<maintainer-github-handle>', maintainer)
    )
    if source_path is not None:
        quoted_source_path = json.dumps(str(source_path.resolve()))
        rendered = re.sub(
            r'source:\n  url: .+\n  sha256: .+\n',
            f'source:\n  path: {quoted_source_path}\n',
            rendered,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding='utf-8')


def main() -> int:
    """Render the conda recipe template and return a process exit code."""
    args = _parser().parse_args()
    render_recipe(
        template_path=Path(args.template),
        output_path=Path(args.output),
        version=args.version,
        sha256=args.sha256,
        maintainer=args.maintainer,
        source_path=Path(args.source_path) if args.source_path else None,
    )
    return 0


# SECTION: MAIN ENTRY POINT ================================================= #


if __name__ == '__main__':
    raise SystemExit(main())
