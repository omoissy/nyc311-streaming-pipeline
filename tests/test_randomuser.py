from __future__ import annotations

import pytest
import requests

from user_stream.randomuser import (
    RETRY_STATUSES,
    RandomUserClient,
    RandomUserError,
    build_session,
)


class FakeResponse:
    def __init__(self, payload=None, status=200, body_is_json=True):
        self._payload = payload
        self.status_code = status
        self._body_is_json = body_is_json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)

    def json(self):
        if not self._body_is_json:
            raise ValueError("Expecting value")
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.response


def client_for(response) -> tuple[RandomUserClient, FakeSession]:
    session = FakeSession(response)
    return RandomUserClient("https://api.test/", timeout=3.0, session=session), session


def test_returns_results_and_sends_count_and_timeout(api_response):
    client, session = client_for(FakeResponse(api_response))

    users = client.fetch_users(2)

    assert [u["login"]["uuid"] for u in users] == [
        "44957db7-9c04-4cff-8020-85aa39e37b4b",
        "0f3c2a9e-6b1d-4e57-9c8a-2d4b6f8e1a30",
    ]
    assert session.calls == [
        {"url": "https://api.test/", "params": {"results": 2}, "timeout": 3.0}
    ]


def test_api_error_payload_raises():
    client, _ = client_for(FakeResponse({"error": "Uh oh, something has gone wrong."}))
    with pytest.raises(RandomUserError, match="something has gone wrong"):
        client.fetch_users(1)


@pytest.mark.parametrize("payload", [{"info": {}}, {"results": "nope"}, ["a", "list"]])
def test_unexpected_payload_shape_raises(payload):
    client, _ = client_for(FakeResponse(payload))
    with pytest.raises(RandomUserError):
        client.fetch_users(1)


def test_non_json_body_raises():
    client, _ = client_for(FakeResponse(body_is_json=False))
    with pytest.raises(RandomUserError, match="not valid JSON"):
        client.fetch_users(1)


def test_http_error_propagates():
    client, _ = client_for(FakeResponse({}, status=503))
    with pytest.raises(requests.HTTPError):
        client.fetch_users(1)


def test_count_must_be_positive():
    client, session = client_for(FakeResponse({"results": []}))
    with pytest.raises(ValueError):
        client.fetch_users(0)
    assert session.calls == []


def test_session_retries_transient_failures():
    retry = build_session(max_retries=4, backoff_factor=0.5).get_adapter("https://x").max_retries

    assert retry.total == 4
    assert retry.backoff_factor == 0.5
    assert set(RETRY_STATUSES) <= set(retry.status_forcelist)
    assert 429 in retry.status_forcelist
    assert retry.respect_retry_after_header
