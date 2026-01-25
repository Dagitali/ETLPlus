"""
:mod:`tests.unit.test_u_main` module.

Unit tests for :mod:`etlplus.__main__`.

Covers CLI entrypoint and _run().
"""

from __future__ import annotations

import pytest

from etlplus import __main__

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# pylint: disable=protected-access
# pylint: disable=unused-argument


# SECTION: TESTS ============================================================ #


def test_run_invokes_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that :func:`_run` invokes :func:`main` and returns its value."""
    called: dict[str, bool] = {}

    def fake_main():
        called['main'] = True
        return 42

    monkeypatch.setattr(__main__, 'main', fake_main)
    assert __main__._run() == 42
    assert called['main']


def test_main_guard_executes_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that the main guard executes :func:`_run` and raises
    :class:`SystemExit`.
    """

    def _run() -> int:
        return 123

    code = "if __name__ == '__main__':\n    raise SystemExit(_run())"
    allowed_globals = {'__name__': '__main__', '_run': _run}

    with pytest.raises(SystemExit) as exc:
        # pylint: disable-next=exec-used
        exec(code, allowed_globals)

    assert exc.value.code == 123
