"""Orquestación del ETL: carga histórica (backfill) e incremental.

Refactor de ``pipelines/secop_pipeline.py``. Diferencias clave:

* el estado vive en el object store, no en disco;
* cada partición se **compacta** (upsert por ``id_contrato``) en vez de acumular
  archivos ``part-*`` duplicados;
* el backfill descarga por mes para acotar memoria y es **reanudable**;
* **nunca se borran contratos**: si el origen los elimina, siguen en el lake.

Reglas entre particiones (``_apply_batch``):

* ``year=unknown`` -> año real: la versión con fecha se escribe en su año y la
  copia sin fecha sale de ``unknown`` hacia el historial (``fecha_completada``);
* año A -> año B: se conservan ambas copias (evidencia de cambio en el origen);
* año real -> sin fecha: se conserva la versión con fecha y la nueva va al
  historial (``fecha_eliminada_en_origen``).
"""

from __future__ import annotations

import json
import logging
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pandas as pd

from secop import history, lake
from secop import index as id_index
from secop.config import (
    DATASET_ID,
    MANIFEST_KEY,
    PARTITION_DATE_COLUMN,
    PRIMARY_KEY,
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
# Escritura de un lote con reglas entre particiones                           #
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _Batch:
    """Lote ya clasificado por partición destino."""

    to_write: pd.DataFrame
    targets: pd.Series
    dated_ids: set[str]
    lost_date: pd.DataFrame


def _classify(idx: pd.DataFrame, df: pd.DataFrame) -> _Batch:
    targets = lake.assign_partitions(df)
    unknown = targets == lake.UNKNOWN_PARTITION
    dated_ids = set(df.loc[~unknown, PRIMARY_KEY].dropna().astype(str))
    in_lake_dated = idx.loc[
        idx[id_index.COL_PARTITION] != lake.UNKNOWN_PARTITION, PRIMARY_KEY
    ]
    # Versiones sin fecha de contratos que sí tienen fecha (en el lake o en el
    # mismo lote): no van a year=unknown, se guardan en el historial.
    has_date = df[PRIMARY_KEY].isin(dated_ids) | df[PRIMARY_KEY].isin(in_lake_dated)
    lost = unknown & has_date
    return _Batch(
        to_write=df[~lost],
        targets=targets[~lost],
        dated_ids=dated_ids,
        lost_date=df[lost],
    )


def _log_year_changes(idx: pd.DataFrame, df: pd.DataFrame, targets: pd.Series) -> None:
    """Avisa de contratos que el origen pasó de un año real a otro (se conservan ambos)."""
    dated = idx[idx[id_index.COL_PARTITION] != lake.UNKNOWN_PARTITION]
    incoming = pd.DataFrame(
        {PRIMARY_KEY: df[PRIMARY_KEY].astype("string"), "target": targets}
    )
    incoming = incoming[incoming["target"] != lake.UNKNOWN_PARTITION]
    merged = incoming.merge(dated, on=PRIMARY_KEY)
    changed = merged[merged["target"] != merged[id_index.COL_PARTITION]]
    if not changed.empty:
        logger.warning(
            "%d contratos cambiaron de año de inicio en el origen; se conservan "
            "ambas copias (p.ej. %s)",
            changed[PRIMARY_KEY].nunique(),
            ", ".join(changed[PRIMARY_KEY].astype(str).unique()[:5]),
        )


def _apply_batch(
    store: ObjectStore, idx: pd.DataFrame, df: pd.DataFrame, detected_at: datetime
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Escribe un lote en el lake sin borrar nada. Devuelve (índice, filas/partición)."""
    written: dict[str, int] = {}
    if df.empty or PRIMARY_KEY not in df.columns:
        return idx, written
    batch = _classify(idx, df)
    _log_year_changes(idx, batch.to_write, batch.targets)
    if not batch.lost_date.empty:
        history.append(
            store,
            batch.lost_date,
            partition=lake.UNKNOWN_PARTITION,
            motivo=history.MOTIVO_FECHA_ELIMINADA,
            detected_at=detected_at,
        )
    for partition, group in batch.to_write.groupby(batch.targets):
        rows = lake.upsert_partition(
            store, str(partition), group, detected_at=detected_at
        )
        idx = id_index.replace_partition(idx, str(partition), rows)
        written[str(partition)] = len(rows)
    # Después de escribir las versiones con fecha: sacar sus copias de unknown.
    remaining = lake.move_out_ids(
        store,
        lake.UNKNOWN_PARTITION,
        batch.dated_ids,
        motivo=history.MOTIVO_FECHA_COMPLETADA,
        detected_at=detected_at,
    )
    if remaining is not None:
        idx = id_index.replace_partition(idx, lake.UNKNOWN_PARTITION, remaining)
        written[lake.UNKNOWN_PARTITION] = len(remaining)
    return idx, written


def _record(result: RunResult, written: dict[str, int]) -> None:
    for partition, rows in written.items():
        result.partitions[partition] = rows
        result.rows_written += rows


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
    """Carga histórica, un año a la vez. Reanudable.

    Siempre fusiona con lo que ya hay en el lake: ``force`` vuelve a descargar
    años existentes, pero nunca elimina contratos que el origen ya no publique.
    """
    result = RunResult(mode="backfill")
    years = (
        [only_year] if only_year is not None else list(range(start_year, end_year + 1))
    )
    max_wm: datetime | None = None
    idx = id_index.load_or_build(store)
    run_at = datetime.now(UTC)

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

        idx, written = _apply_batch(store, idx, year_df, run_at)
        _record(result, written)
        wm = _batch_watermark(year_df)
        if wm and (max_wm is None or wm > max_wm):
            max_wm = wm

    if only_year is None:
        logger.info(
            "Descargando registros fuera de rango (nulos / previos / futuros)..."
        )
        tail_df = _fetch_tail(start_year, end_year)
        idx, written = _apply_batch(store, idx, tail_df, run_at)
        _record(result, written)
        wm = _batch_watermark(tail_df)
        if wm and (max_wm is None or wm > max_wm):
            max_wm = wm

    id_index.save(store, idx)
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

    idx = id_index.load_or_build(store)
    idx, written = _apply_batch(store, idx, df, datetime.now(UTC))
    _record(result, written)
    state.row_counts.update(written)
    id_index.save(store, idx)

    new_wm = _batch_watermark(df)
    if new_wm is not None and new_wm > state.watermark:
        state.ultima_actualizacion = new_wm.isoformat()
    state.schema_version = SCHEMA_VERSION
    save_state(store, state)
    _write_manifest(store, state)

    result.new_watermark = state.ultima_actualizacion
    return result
