from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.util import parse_url

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


def _assert_end_user_header(headers: dict, end_user: str | None) -> None:
    if end_user is None:
        assert VastdbApi.END_USER_HEADER not in headers
    else:
        assert headers[VastdbApi.END_USER_HEADER] == end_user


@pytest.mark.parametrize("end_user", ["mock-end-user", None])
@pytest.mark.parametrize("headers", [None, {"tabular-txid": "0"}])
def test_single_request_sends_end_user(end_user: str | None, headers: dict[str, str] | None):
    api = _make_api(end_user)
    response = requests.Response()
    response.status_code = 200
    session = MagicMock()
    session.request.return_value = response

    with patch.object(api, "_session_get_or_create", return_value=session):
        api._single_request(method="HEAD", url=parse_url("http://blabla:1234/my-bucket"), headers=headers)

    sent_headers = session.request.call_args.kwargs.get("headers") or {}
    if headers is not None:
        assert sent_headers["tabular-txid"] == "0"
    _assert_end_user_header(sent_headers, end_user)


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
