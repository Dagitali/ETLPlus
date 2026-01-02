"""
:mod:`etlplus.__main__` module.

Thin wrapper supporting `python -m etlplus` by delegating to the CLI
entrypoint.
"""

from .cli import main

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _run() -> int:
    """Return the exit status."""
    return main()


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':  # pragma: no cover - exercised via CLI
    raise SystemExit(_run())
