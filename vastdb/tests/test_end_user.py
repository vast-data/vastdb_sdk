from unittest.mock import patch

import pytest

from vastdb._internal import VastdbApi
from vastdb.session import Session


def _make_api(end_user: str | None = None) -> VastdbApi:
    return VastdbApi(
        access_key="ak",
        secret_key="sk",
        endpoints=["http://blabla:1234"],
        end_user=end_user,
        max_entities_per_page=1000,
        version_check=False,
    )


@pytest.mark.parametrize("end_user", ["mock-end-user", None])
def test_fill_common_headers_includes_end_user(end_user: str | None):
    api = _make_api(end_user)
    headers = api._fill_common_headers()

    if end_user is None:
        assert VastdbApi.END_USER_HEADER not in headers
    else:
        assert headers[VastdbApi.END_USER_HEADER] == end_user


@pytest.mark.parametrize("end_user", ["mock-end-user", None])
def test_session_passes_end_user(end_user: str | None):
    with patch("vastdb._internal.VastdbApi") as MockVastdbApi:
        mock_api = MockVastdbApi.return_value
        mock_api.vast_version = (5, 4, 0, 0)

        session = Session(
            access="test_access",
            secret="test_secret",
            endpoint="http://blabla:1234",
            end_user=end_user,
        )

        assert MockVastdbApi.call_args.kwargs["end_user"] == end_user
        assert session._context.end_user == end_user


@pytest.mark.parametrize("end_user", ["mock-end-user", None])
def test_with_endpoints_preserves_end_user(end_user: str | None):
    api = _make_api(end_user)
    cloned = api.with_endpoints(["http://blabla:4321"])

    assert cloned._end_user == end_user
    headers = cloned._fill_common_headers()
    if end_user is None:
        assert VastdbApi.END_USER_HEADER not in headers
    else:
        assert headers[VastdbApi.END_USER_HEADER] == end_user
