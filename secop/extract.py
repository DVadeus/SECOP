"""Cliente de extracción de la API Socrata de datos.gov.co.

Migrado de ``src/extract.py``. Cambios: paginación estable con ``$order=:id``,
inyección de dependencias para testear, y uso de ``get_settings()``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from secop.config import SOCRATA_RESOURCE_URL, get_settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2.0
REQUEST_TIMEOUT = 120

# Socrata exige un $order determinista para que $offset no salte ni repita filas
# entre páginas. ":id" es la fila-id interna del dataset, siempre presente.
STABLE_ORDER = ":id"


def _headers() -> dict[str, str]:
    token = get_settings().app_token
    return {"X-App-Token": token} if token else {}


def _request_with_retry(
    url: str,
    params: dict[str, Any],
    session: requests.Session,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url, headers=_headers(), params=params, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Intento %d/%d fallido: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise requests.RequestException(
        f"Fallo tras {MAX_RETRIES} intentos contra {url}"
    ) from last_exc


def iter_pages(
    where_clause: str,
    *,
    session: requests.Session | None = None,
    page_size: int | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Itera páginas de registros crudos para una cláusula ``$where`` dada."""
    owns_session = session is None
    session = session or requests.Session()
    size = page_size or get_settings().socrata_page_size
    offset = 0
    downloaded = 0
    try:
        while True:
            params: dict[str, Any] = {
                "$where": where_clause,
                "$order": STABLE_ORDER,
                "$limit": size,
                "$offset": offset,
            }
            page = _request_with_retry(SOCRATA_RESOURCE_URL, params, session).json()
            if not page:
                break
            downloaded += len(page)
            logger.info("Descargados %d registros (offset %d)", downloaded, offset)
            yield page
            if len(page) < size:
                break
            offset += size
    finally:
        if owns_session:
            session.close()


def fetch_data(
    where_clause: str,
    *,
    session: requests.Session | None = None,
    page_size: int | None = None,
) -> list[dict[str, Any]]:
    """Descarga todos los registros que cumplen ``where_clause`` (materializado)."""
    records: list[dict[str, Any]] = []
    for page in iter_pages(where_clause, session=session, page_size=page_size):
        records.extend(page)
    return records
