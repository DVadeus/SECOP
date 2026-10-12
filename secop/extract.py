"""Cliente de extracción de la API Socrata de datos.gov.co.

Migrado de ``src/extract.py``. Cambios: paginación estable con ``$order=:id``,
inyección de dependencias para testear, y uso de ``get_settings()``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Sequence
from typing import Any

import pandas as pd
import requests

from secop.bases import CONTRATOS, Base
from secop.config import get_settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2.0
REQUEST_TIMEOUT = 120

# Socrata exige un $order determinista para que $offset no salte ni repita filas
# entre páginas. ":id" es la fila-id interna del dataset, siempre presente.
STABLE_ORDER = ":id"

# Ids por petición en fetch_by_ids: acota el largo de la URL del "in (...)".
ID_BATCH_SIZE = 200


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
    select: str | None = None,
    base: Base = CONTRATOS,
) -> Iterator[list[dict[str, Any]]]:
    """Itera páginas de registros crudos para una cláusula ``$where`` dada.

    ``select`` limita las columnas (``$select``) para descargas ligeras.
    """
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
            if select:
                params["$select"] = select
            page = _request_with_retry(base.resource_url, params, session).json()
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
    base: Base = CONTRATOS,
) -> list[dict[str, Any]]:
    """Descarga todos los registros que cumplen ``where_clause`` (materializado)."""
    records: list[dict[str, Any]] = []
    for page in iter_pages(
        where_clause, session=session, page_size=page_size, base=base
    ):
        records.extend(page)
    return records


# --------------------------------------------------------------------------- #
# Foto ligera del origen y descarga dirigida por id                           #
# --------------------------------------------------------------------------- #
class SourceReloadedError(RuntimeError):
    """El origen se recargó durante una descarga paginada (páginas inconsistentes)."""


def source_version(
    session: requests.Session | None = None, base: Base = CONTRATOS
) -> int | None:
    """Marca de recarga del dataset (``rowsUpdatedAt`` de la API de metadatos).

    datos.gov.co recarga el dataset completo a diario y ``:id`` cambia: si la
    marca cambia durante una paginación, el resultado no es fiable.
    """
    owns_session = session is None
    session = session or requests.Session()
    try:
        meta = _request_with_retry(base.metadata_url, {}, session).json()
    finally:
        if owns_session:
            session.close()
    value = meta.get("rowsUpdatedAt") if isinstance(meta, dict) else None
    return int(value) if value is not None else None


def fetch_snapshot(
    columns: Sequence[str],
    *,
    session: requests.Session | None = None,
    page_size: int | None = None,
    base: Base = CONTRATOS,
) -> pd.DataFrame:
    """Descarga ``columns`` de TODO el dataset (sin filas completas).

    Lanza :class:`SourceReloadedError` si el origen se recargó mientras tanto.
    """
    before = source_version(session, base)
    frames = [pd.DataFrame(columns=list(columns))]
    for page in iter_pages(
        "1=1",
        session=session,
        page_size=page_size,
        select=", ".join(columns),
        base=base,
    ):
        frames.append(pd.DataFrame.from_records(page, columns=list(columns)))
    _ensure_not_reloaded(before, source_version(session, base))
    return pd.concat(frames, ignore_index=True)


def _ensure_not_reloaded(before: int | None, after: int | None) -> None:
    if before != after:
        raise SourceReloadedError(
            f"El origen se recargó durante la descarga ({before} -> {after}); "
            "vuelve a ejecutar."
        )


def fetch_all_checked(
    *,
    session: requests.Session | None = None,
    page_size: int | None = None,
    base: Base = CONTRATOS,
) -> list[dict[str, Any]]:
    """Descarga TODAS las filas completas de ``base`` (datasets pequeños).

    Lanza :class:`SourceReloadedError` si el origen se recargó mientras tanto.
    """
    before = source_version(session, base)
    records = fetch_data("1=1", session=session, page_size=page_size, base=base)
    _ensure_not_reloaded(before, source_version(session, base))
    logger.info("Descargadas %d filas de %s", len(records), base.name)
    return records


def _soql_in(column: str, values: Sequence[str]) -> str:
    quoted = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return f"{column} in ({quoted})"


def fetch_by_ids(
    ids: Sequence[str],
    *,
    session: requests.Session | None = None,
    batch_size: int = ID_BATCH_SIZE,
    base: Base = CONTRATOS,
) -> list[dict[str, Any]]:
    """Descarga las filas completas de ``ids`` en lotes de ``batch_size``."""
    owns_session = session is None
    session = session or requests.Session()
    records: list[dict[str, Any]] = []
    try:
        for start in range(0, len(ids), batch_size):
            chunk = ids[start : start + batch_size]
            where = _soql_in(base.key, chunk)
            records.extend(fetch_data(where, session=session, base=base))
    finally:
        if owns_session:
            session.close()
    logger.info("Descargados %d registros de %d ids", len(records), len(ids))
    return records
