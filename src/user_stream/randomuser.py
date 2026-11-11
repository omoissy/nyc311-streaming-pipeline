"""HTTP client for the randomuser.me API with retries and exponential backoff."""

from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRY_STATUSES = (429, 500, 502, 503, 504)


class RandomUserError(RuntimeError):
    """The API responded, but not with usable data."""


def build_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Session that retries transient failures (connection errors, 429, 5xx) with backoff."""
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = "realtime-data-streaming-pipeline/1.0"
    return session


class RandomUserClient:
    def __init__(
        self,
        base_url: str = "https://randomuser.me/api/",
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.session = session or build_session()

    def fetch_users(self, count: int) -> list[dict[str, Any]]:
        """Return `count` raw user records. Raises on HTTP errors or a malformed payload."""
        if count < 1:
            raise ValueError("count must be at least 1")
        response = self.session.get(
            self.base_url, params={"results": count}, timeout=self.timeout
        )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RandomUserError("response body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise RandomUserError(f"unexpected payload type: {type(payload).__name__}")
        if "error" in payload:
            raise RandomUserError(f"API returned an error: {payload['error']}")
        results = payload.get("results")
        if not isinstance(results, list):
            raise RandomUserError("payload is missing the 'results' list")
        return results
