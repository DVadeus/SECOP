"""Orquestación del ETL: carga histórica (backfill) e incremental.

Refactor de ``pipelines/secop_pipeline.py``. Diferencias clave:

* el estado vive en el object store, no en disco;
* cada partición se **compacta** (upsert por ``id_contrato``) en vez de acumular
  archivos ``part-*`` duplicados;
* el backfill descarga por mes para acotar memoria y es **reanudable**.
"""

from __future__ import annotations

import json
import logging
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pandas as pd

from secop import lake
from secop.config import (
    DATASET_ID,
    MANIFEST_KEY,
    PARTITION_DATE_COLUMN,
    WATERMARK_COLUMN,
)
from secop.extract import fetch_data
from secop.schema import SCHEMA_VERSION
from secop.state import PipelineState, load_state, save_state
from secop.storage.base import ObjectStore
from secop.transform import to_dataframe

logger = logging.getLogger(__name__)

_SOCRATA_TS = "%Y-%m-%dT%H:%M:%S"


@dataclass(slots=True)
class RunResult:
    """Resumen de una corrida del pipeline."""

    mode: str
    rows_written: int = 0
    partitions: dict[str, int] = field(default_factory=dict)
    new_watermark: str | None = None
    skipped: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Helpers de extracción por ventana temporal                                  #
# --------------------------------------------------------------------------- #
def _month_windows(year: int) -> list[tuple[str, str]]:
    windows: list[tuple[str, str]] = []
    for month in range(1, 13):
        last_day = monthrange(year, month)[1]
        start = f"{year:04d}-{month:02d}-01T00:00:00"
        end_dt = datetime(year, month, last_day) + timedelta(days=1)
        windows.append((start, end_dt.strftime(_SOCRATA_TS)))
    return windows


def _fetch_frame(where_clause: str) -> pd.DataFrame:
    return to_dataframe(fetch_data(where_clause))


def _fetch_year(year: int) -> pd.DataFrame:
    """Descarga un año completo, mes a mes, y lo concatena."""
    frames: list[pd.DataFrame] = []
    for start, end in _month_windows(year):
        where = f"{PARTITION_DATE_COLUMN} >= '{start}' AND {PARTITION_DATE_COLUMN} < '{end}'"
        month_df = _fetch_frame(where)
        if not month_df.empty:
            frames.append(month_df)
            logger.info("  %s..%s -> %d filas", start[:7], end[:7], len(month_df))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_tail(start_year: int, end_year: int) -> pd.DataFrame:
    """Registros fuera del rango de años del bucle: fechas nulas, previas o futuras."""
    where = (
        f"{PARTITION_DATE_COLUMN} IS NULL "
        f"OR {PARTITION_DATE_COLUMN} < '{start_year:04d}-01-01T00:00:00' "
        f"OR {PARTITION_DATE_COLUMN} >= '{end_year + 1:04d}-01-01T00:00:00'"
    )
    return _fetch_frame(where)


# --------------------------------------------------------------------------- #
# Manifest                                                                    #
# --------------------------------------------------------------------------- #
def _write_manifest(store: ObjectStore, state: PipelineState) -> None:
    manifest = {
        "dataset_id": DATASET_ID,
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "watermark": state.ultima_actualizacion,
        "row_counts": state.row_counts,
        "total_rows": sum(state.row_counts.values()),
    }
    store.put_bytes(
        MANIFEST_KEY, json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    )


def _batch_watermark(df: pd.DataFrame) -> datetime | None:
    if WATERMARK_COLUMN not in df.columns:
        return None
    col = pd.to_datetime(df[WATERMARK_COLUMN], errors="coerce")
    top = col.max()
    if pd.isna(top):
        return None
    ts = top.to_pydatetime()
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Backfill                                                                    #
# --------------------------------------------------------------------------- #
def backfill(
    store: ObjectStore,
    *,
    start_year: int,
    end_year: int,
    force: bool = False,
    only_year: int | None = None,
) -> RunResult:
    """Carga histórica: un ``data.parquet`` compactado por año. Reanudable."""
    result = RunResult(mode="backfill")
    years = (
        [only_year] if only_year is not None else list(range(start_year, end_year + 1))
    )
    max_wm: datetime | None = None

    for year in years:
        partition = f"year={year}"
        if not force and store.exists(lake.data_key(partition)):
            logger.info("Año %d ya presente, se omite (usa --force para rehacer)", year)
            result.skipped.append(partition)
            continue

        logger.info("Descargando año %d...", year)
        year_df = _fetch_year(year)
        if year_df.empty:
            logger.warning("Año %d sin registros", year)
            continue

        written = lake.write_new_partition(store, partition, year_df)
        result.partitions[partition] = written
        result.rows_written += written
        wm = _batch_watermark(year_df)
        if wm and (max_wm is None or wm > max_wm):
            max_wm = wm

    if only_year is None:
        logger.info(
            "Descargando registros fuera de rango (nulos / previos / futuros)..."
        )
        tail_df = _fetch_tail(start_year, end_year)
        if not tail_df.empty:
            for partition, group in tail_df.groupby(lake.assign_partitions(tail_df)):
                written = lake.upsert_partition(store, str(partition), group)
                result.partitions[str(partition)] = written
                result.rows_written += written
            wm = _batch_watermark(tail_df)
            if wm and (max_wm is None or wm > max_wm):
                max_wm = wm

    state = load_state(store)
    # Conteos: los que se escribieron en esta corrida ya los tenemos; evita
    # releer todo el lake desde R2 sólo para contar filas.
    if only_year is None:
        state.row_counts = dict(result.partitions)
    else:
        state.row_counts.update(result.partitions)
    # Sólo el backfill completo fija el watermark inicial. Un --year puntual no lo
    # mueve (podría adelantarlo más allá de lo realmente descargado en otros años).
    if max_wm is not None and only_year is None:
        state.ultima_actualizacion = max_wm.isoformat()
    state.schema_version = SCHEMA_VERSION
    save_state(store, state)
    _write_manifest(store, state)

    result.new_watermark = state.ultima_actualizacion
    return result


# --------------------------------------------------------------------------- #
# Incremental                                                                 #
# --------------------------------------------------------------------------- #
def incremental(store: ObjectStore, *, lookback_days: int = 2) -> RunResult:
    """Descarga sólo lo cambiado desde el watermark y hace upsert por partición."""
    result = RunResult(mode="incremental")
    state = load_state(store)
    since = state.watermark - timedelta(days=lookback_days)
    where = f"{WATERMARK_COLUMN} > '{since.strftime(_SOCRATA_TS)}'"
    logger.info("Extrayendo cambios desde %s (lookback %dd)", since, lookback_days)

    df = _fetch_frame(where)
    if df.empty:
        logger.info("No hay registros nuevos")
        result.new_watermark = state.ultima_actualizacion
        return result

    df = lake.compact(df)  # dedup dentro del lote antes de tocar el lake
    for partition, group in df.groupby(lake.assign_partitions(df)):
        written = lake.upsert_partition(store, str(partition), group)
        state.row_counts[str(partition)] = written
        result.partitions[str(partition)] = written
        result.rows_written += written

    new_wm = _batch_watermark(df)
    if new_wm is not None and new_wm > state.watermark:
        state.ultima_actualizacion = new_wm.isoformat()
    state.schema_version = SCHEMA_VERSION
    save_state(store, state)
    _write_manifest(store, state)

    result.new_watermark = state.ultima_actualizacion
    return result
