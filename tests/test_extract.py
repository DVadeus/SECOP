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


# --------------------------------------------------------------------------- #
# Foto ligera, recarga del origen y descarga por id                           #
# --------------------------------------------------------------------------- #
class _RoutingSession:
    """Responde metadatos y datos por URL; registra los parámetros enviados."""

    def __init__(self, versions: list[int], pages: list[list[dict[str, Any]]]) -> None:
        self._versions = iter(versions)
        self._pages = iter(pages)
        self.data_calls: list[dict[str, Any]] = []

    def get(self, url: str, headers=None, params=None, timeout=None) -> Any:  # noqa: ANN001
        if url.startswith("https://www.datos.gov.co/api/views/"):
            return _FakeResponse({"rowsUpdatedAt": next(self._versions)})  # type: ignore[arg-type]
        self.data_calls.append(dict(params or {}))
        return _FakeResponse(next(self._pages, []))

    def close(self) -> None:  # pragma: no cover
        pass


def test_iter_pages_sends_select() -> None:
    session = _FakeSession([[{"i": 1}]])
    list(extract.iter_pages("1=1", session=session, page_size=10, select="a, b"))  # type: ignore[arg-type]
    assert session.calls[0]["$select"] == "a, b"


def test_fetch_snapshot_returns_requested_columns() -> None:
    session = _RoutingSession(
        versions=[7, 7],
        pages=[[{"id_contrato": "A"}, {"id_contrato": "B", "x": "1"}]],
    )
    snap = extract.fetch_snapshot(["id_contrato", "x"], session=session, page_size=5)  # type: ignore[arg-type]
    assert snap.columns.tolist() == ["id_contrato", "x"]
    assert snap["id_contrato"].tolist() == ["A", "B"]
    assert session.data_calls[0]["$select"] == "id_contrato, x"


def test_fetch_snapshot_detects_source_reload() -> None:
    session = _RoutingSession(versions=[1, 2], pages=[[{"id_contrato": "A"}]])
    with pytest.raises(extract.SourceReloadedError):
        extract.fetch_snapshot(["id_contrato"], session=session, page_size=5)  # type: ignore[arg-type]


def test_fetch_by_ids_batches_and_escapes_quotes() -> None:
    session = _FakeSession([[{"id_contrato": "A"}], [{"id_contrato": "O'B"}]])
    rows = extract.fetch_by_ids(["A", "O'B"], session=session, batch_size=1)  # type: ignore[arg-type]
    assert [r["id_contrato"] for r in rows] == ["A", "O'B"]
    assert session.calls[0]["$where"] == "id_contrato in ('A')"
    assert session.calls[1]["$where"] == "id_contrato in ('O''B')"


def test_fetch_all_checked_uses_base_url_and_detects_reload() -> None:
    from secop.bases import INTERADMINISTRATIVOS

    class _UrlSession(_RoutingSession):
        def __init__(self, versions: list[int], pages: list[list[dict[str, Any]]]):
            super().__init__(versions, pages)
            self.urls: list[str] = []

        def get(self, url: str, headers=None, params=None, timeout=None) -> Any:  # noqa: ANN001
            self.urls.append(url)
            return super().get(url, headers=headers, params=params, timeout=timeout)

    ok = _UrlSession(versions=[5, 5], pages=[[{"id_contrato": "A"}]])
    rows = extract.fetch_all_checked(
        session=ok, page_size=10, base=INTERADMINISTRATIVOS
    )  # type: ignore[arg-type]
    assert rows == [{"id_contrato": "A"}]
    assert any("s484-c9k3" in u for u in ok.urls)
    assert all("jbjy-vk9h" not in u for u in ok.urls)

    reloaded = _UrlSession(versions=[5, 6], pages=[[{"id_contrato": "A"}]])
    with pytest.raises(extract.SourceReloadedError):
        extract.fetch_all_checked(
            session=reloaded, page_size=10, base=INTERADMINISTRATIVOS
        )  # type: ignore[arg-type]
