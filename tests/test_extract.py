"""Pruebas de secop.extract (paginación + reintentos, con requests mockeado)."""

from __future__ import annotations

from typing import Any

import pytest
import requests

from secop import extract


class _FakeResponse:
    def __init__(self, payload: list[dict[str, Any]], status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self) -> list[dict[str, Any]]:
        return self._payload


class _FakeSession:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = pages
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, headers=None, params=None, timeout=None) -> _FakeResponse:  # noqa: ANN001
        self.calls.append(params or {})
        idx = len(self.calls) - 1
        page = self._pages[idx] if idx < len(self._pages) else []
        return _FakeResponse(page)

    def close(self) -> None:  # pragma: no cover
        pass


def test_paginates_until_short_page() -> None:
    session = _FakeSession([[{"i": n} for n in range(3)], [{"i": 3}]])
    rows = extract.fetch_data("1=1", session=session, page_size=3)  # type: ignore[arg-type]
    assert [r["i"] for r in rows] == [0, 1, 2, 3]
    assert session.calls[0]["$offset"] == 0
    assert session.calls[1]["$offset"] == 3
    assert session.calls[0]["$order"] == extract.STABLE_ORDER


def test_stops_on_empty_first_page() -> None:
    session = _FakeSession([[]])
    assert extract.fetch_data("1=1", session=session, page_size=10) == []  # type: ignore[arg-type]


def test_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"n": 0}

    def flaky_get(*_args: Any, **_kwargs: Any) -> _FakeResponse:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise requests.ConnectionError("boom")
        return _FakeResponse([])

    monkeypatch.setattr(extract.time, "sleep", lambda _s: None)
    session = _FakeSession([])
    monkeypatch.setattr(session, "get", flaky_get)

    extract.fetch_data("1=1", session=session, page_size=10)  # type: ignore[arg-type]
    assert attempts["n"] == 3


def test_raises_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    def always_fail(*_args: Any, **_kwargs: Any) -> _FakeResponse:
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(extract.time, "sleep", lambda _s: None)
    session = _FakeSession([])
    monkeypatch.setattr(session, "get", always_fail)

    with pytest.raises(requests.RequestException):
        extract.fetch_data("1=1", session=session, page_size=10)  # type: ignore[arg-type]
