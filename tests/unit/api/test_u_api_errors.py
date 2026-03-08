"""
:mod:`tests.unit.api.test_u_api_errors` module.

Unit tests for :mod:`etlplus.api.errors`.
"""

from __future__ import annotations

from etlplus.api.errors import ApiRequestError
from etlplus.api.errors import PaginationError

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


class TestApiErrors:
    """Unit tests for API error payload helpers."""

    def test_api_request_error_as_dict_includes_cause_repr(self) -> None:
        """Test that structured payload includes serialized cause context."""
        err = ApiRequestError(
            url='https://example.test/u',
            status=500,
            attempts=2,
            retried=True,
            retry_policy={'max_attempts': 3},
            cause=ValueError('boom'),
        )
        payload = err.as_dict()
        assert payload['url'] == 'https://example.test/u'
        assert payload['status'] == 500
        assert payload['attempts'] == 2
        assert payload['retried'] is True
        assert payload['retry_policy'] == {'max_attempts': 3}
        assert 'ValueError' in str(payload['cause'])

    def test_pagination_error_as_dict_adds_page(self) -> None:
        """
        Test that :class:`PaginationError` extends base payload with page
        metadata.
        """
        err = PaginationError(
            url='https://example.test/u',
            status=429,
            page=3,
        )
        payload = err.as_dict()
        assert payload['url'] == 'https://example.test/u'
        assert payload['page'] == 3
