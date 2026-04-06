"""
:mod:`tests.unit.test_u_main` module.

Unit tests for :mod:`etlplus.__main__`.
"""

from __future__ import annotations

import pytest

from etlplus import __main__

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: TESTS ============================================================ #


class TestMainEntrypoint:
    """Unit tests for the package CLI entrypoint."""

    def test_guard_executes_run(self) -> None:
        """
        Test that the ``__main__`` guard raises :class:`SystemExit` with
        :func:`_run`'s code.
        """

        def _run() -> int:
            return 123

        code = "if __name__ == '__main__':\n    raise SystemExit(_run())"
        allowed_globals = {'__name__': '__main__', '_run': _run}

        with pytest.raises(SystemExit) as exc:
            # pylint: disable-next=exec-used
            exec(code, allowed_globals)

        assert exc.value.code == 123

    def test_run_invokes_main(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`_run` invokes :func:`main` and returns its value.
        """
        called: dict[str, bool] = {}

        def fake_main() -> int:
            called['main'] = True
            return 42

        monkeypatch.setattr(__main__, 'main', fake_main)

        assert __main__._run() == 42
        assert called['main'] is True
