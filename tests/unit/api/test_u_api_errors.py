"""
:mod:`tests.unit.api.test_u_api_errors` module.

Unit tests for :mod:`etlplus.api._errors`.
"""

from __future__ import annotations

import pytest

from etlplus.api._errors import ApiRequestError
from etlplus.api._errors import PaginationError

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


class TestApiErrors:
    """Unit tests for API error payload helpers."""

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('url', 'https://example.test/u', id='url'),
            pytest.param('status', 500, id='status'),
            pytest.param('attempts', 2, id='attempts'),
            pytest.param('retried', True, id='retried'),
            pytest.param('retry_policy', {'max_attempts': 3}, id='retry-policy'),
            pytest.param('cause_contains', 'ValueError', id='cause'),
        ],
    )
    def test_api_request_error_as_dict_includes_cause_repr(
        self,
        field: str,
        expected: object,
    ) -> None:
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
        if field == 'cause_contains':
            assert str(expected) in str(payload['cause'])
        else:
            assert payload[field] == expected

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
