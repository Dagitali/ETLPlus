#!/usr/bin/env python3
"""Refresh recorded CLI output snippets that appear in DEMO.md."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
import venv
from pathlib import Path

# SECTION: CONSTANTS ======================================================== #


ROOT = Path(__file__).resolve().parents[1]
DEMO_FILE = ROOT / 'DEMO.md'
SNIPPET_FILE = ROOT / 'docs' / 'snippets' / 'installation_version.md'
SNIPPET_FILE.parent.mkdir(parents=True, exist_ok=True)

END_MARKER = '<!-- snippet:end installation_version -->'
START_MARKER = '<!-- snippet:start installation_version -->'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _build_wheel(
    out_dir: Path,
) -> Path:
    """
    Build a wheel into *out_dir* and return its path.

    Requires the 'build' package.

    Parameters
    ----------
    out_dir : Path
        Directory to place the built wheel into.

    Returns
    -------
    Path
        Path to the built wheel.

    Raises
    ------
    RuntimeError
        If ``python -m build`` did not produce a wheel.
    """
    _run([sys.executable, '-m', 'build', '--wheel', '--outdir', str(out_dir)])
    wheels = sorted(out_dir.glob('*.whl'), key=lambda p: p.stat().st_mtime)
    if not wheels:
        raise RuntimeError('No wheel produced by python -m build')
    return wheels[-1]


def _create_venv(
    venv_dir: Path,
) -> tuple[Path, Path]:
    """
    Create a temporary venv and return (pip_path, etlplus_path).

    Parameters
    ----------
    venv_dir : Path
        Directory in which to create the virtual environment.

    Returns
    -------
    tuple[Path, Path]
        Paths to the ``pip`` and ``etlplus`` executables within the venv.
    """
    venv.EnvBuilder(with_pip=True, clear=True).create(venv_dir)
    bin_dir = venv_dir / ('Scripts' if os.name == 'nt' else 'bin')
    pip_path = bin_dir / ('pip.exe' if os.name == 'nt' else 'pip')
    etlplus_path = bin_dir / ('etlplus.exe' if os.name == 'nt' else 'etlplus')
    return pip_path, etlplus_path


def _record_version_output() -> str:
    """
    Build & install the wheel, then capture `etlplus --version`.

    Returns
    -------
    str
        The captured output of `etlplus --version`, suitable for inclusion in a
        Markdown code block.
    """
    with tempfile.TemporaryDirectory(prefix='etlplus-build-') as build_dir:
        wheel_path = _build_wheel(Path(build_dir))
        with tempfile.TemporaryDirectory(prefix='etlplus-venv-') as venv_dir:
            pip_path, etlplus_path = _create_venv(Path(venv_dir))
            _run(
                [str(pip_path), 'install', str(wheel_path)],
                capture_output=True,
            )
            completed = _run(
                [str(etlplus_path), '--version'],
                capture_output=True,
            )
    snippet_body = textwrap.dedent(
        f"""$ etlplus --version
{completed.stdout.strip()}""",
    ).strip()
    return snippet_body + '\n'


def _run(
    cmd: list[str],
    **kwargs,
) -> subprocess.CompletedProcess[str]:
    """
    Run a subprocess with helpful defaults.

    Parameters
    ----------
    cmd : list[str]
        Command and arguments to run.
    **kwargs
        Additional arguments to pass to `subprocess.run()`.

    Returns
    -------
    subprocess.CompletedProcess[str]
        The completed process.
    """
    return subprocess.run(
        cmd,
        check=True,
        text=True,
        cwd=ROOT,
        **kwargs,
    )


def _update_demo(
    snippet_body: str,
) -> None:
    """
    Replace the marked block in DEMO.md with the new snippet.

    Parameters
    ----------
    snippet_body : str
        The snippet content to insert.

    Returns
    -------
    None
        This function modifies DEMO.md in place and does not return anything.

    Raises
    ------
    RuntimeError
        If the snippet markers could not be found.
    """
    text = DEMO_FILE.read_text(encoding='utf-8')
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1:
        raise RuntimeError('Could not locate snippet markers in DEMO.md')
    start += len(START_MARKER)
    replacement = f'\n```bash\n{snippet_body}```\n'
    new_text = text[:start] + replacement + text[end:]
    DEMO_FILE.write_text(new_text, encoding='utf-8')


# SECTION: FUNCTIONS ======================================================== #


def main() -> None:
    """Update the installation & version snippet in DEMO.md."""
    snippet_body = _record_version_output()
    SNIPPET_FILE.write_text(snippet_body, encoding='utf-8')
    _update_demo(snippet_body)
    print(f'Updated {SNIPPET_FILE.relative_to(ROOT)} and DEMO.md')


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':
    main()
