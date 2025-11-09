"""
tests.unit.api.test_client_url_composition_property unit tests module.

Property-based tests for URL composition.

Focus:
* Path parameter encoding (percent-encoding of unsafe chars)
* Query parameter round-trip (decode matches original mapping)

Skipped automatically if Hypothesis is absent.
"""
from __future__ import annotations

import urllib.parse as urlparse
from typing import Any

import pytest

import etlplus.api.client as cmod
from etlplus.api import EndpointClient

# Optional Hypothesis import with safe stubs when missing.
try:  # pragma: no try
    from hypothesis import given  # type: ignore[import-not-found]
    from hypothesis import strategies as st  # type: ignore[import-not-found]
    _HYP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _HYP_AVAILABLE = False

    def given(*_a, **_k):  # type: ignore[unused-ignore]
        def _wrap(fn):
            return pytest.mark.skip(reason='needs hypothesis')(fn)
        return _wrap

    class _Strategy:  # minimal chainable strategy stub
        def filter(self, *_a, **_k):  # pragma: no cover
            return self

    class _DummyStrategies:
        def text(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

        def characters(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

        def dictionaries(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

    st = _DummyStrategies()  # type: ignore[assignment]


@given(
    id_value=st.text(
        alphabet=st.characters(blacklist_categories=('Cs',)),
        min_size=1,
    ),
)
def test_path_parameter_encoding_property(
    id_value: str,
) -> None:
    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_k: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cmod, '_extract', fake_extract)
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'item': '/users/{id}'},
        )
        client.paginate(
            'item', path_parameters={'id': id_value}, pagination=None,
        )
    assert captured, 'no URL captured'
    url = captured.pop()
    parsed = urlparse.urlparse(url)
    expected_id = urlparse.quote(id_value, safe='')
    assert parsed.path.endswith('/users/' + expected_id)


def _ascii_no_amp_eq():  # type: ignore[missing-return-type]
    alpha = st.characters(min_codepoint=32, max_codepoint=126).filter(
        lambda ch: ch not in ['&', '='],
    )
    return st.text(alphabet=alpha, min_size=0, max_size=12)


@given(
    params=st.dictionaries(
        keys=_ascii_no_amp_eq().filter(lambda s: len(s) > 0),
        values=_ascii_no_amp_eq(),
        min_size=1,
        max_size=5,
    ),
)
def test_query_encoding_property(
    params: dict[str, str],
) -> None:
    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_k: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cmod, '_extract', fake_extract)
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'e': '/ep'},
        )
        client.paginate('e', query_parameters=params, pagination=None)
    assert captured, 'no URL captured'
    url = captured.pop()
    parsed = urlparse.urlparse(url)
    round_params = dict(
        urlparse.parse_qsl(parsed.query, keep_blank_values=True),
    )
    assert round_params == params
