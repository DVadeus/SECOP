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

from secop import diff, history, lake
from secop import index as id_index
from secop.config import (
    DATASET_ID,
    MANIFEST_KEY,
    PARTITION_DATE_COLUMN,
    PRIMARY_KEY,
    RUN_MARKER_KEY,
    STATE_KEY,
)
from secop.extract import fetch_by_ids, fetch_data, fetch_snapshot
from secop.schema import SCHEMA_VERSION
from secop.state import PipelineState, load_state, save_state
from secop.storage.base import ObjectStore
from secop.transform import to_dataframe

logger = logging.getLogger(__name__)

_SOCRATA_TS = "%Y-%m-%dT%H:%M:%S"

# Ids por lote aplicado al lake en el incremental: acota la memoria (filas
# completas de ~80 columnas) a costa de reescribir particiones por lote.
FETCH_CHUNK_IDS = 50_000


@dataclass(slots=True)
class RunResult:
    """Resumen de una corrida del pipeline."""

    mode: str
    #: Versiones de contratos aplicadas al lake (nuevas o actualizadas). No es el
    #: tamaño de las particiones: una partición reescrita en varios lotes cuenta
    #: sólo las filas que llegaron.
    contracts_written: int = 0
    #: Filas totales de cada partición reescrita, al terminar la corrida.
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


def _begin(store: ObjectStore, mode: str) -> pd.DataFrame:
    """Marca la corrida como en curso y devuelve un índice fiable.

    Si la marca ya existía, la corrida anterior se cayó tras escribir alguna
    partición pero antes de guardar el índice: se reconstruye desde el lake.
    """
    if store.exists(RUN_MARKER_KEY):
        logger.warning("La corrida anterior no terminó; se reconstruye el índice")
        idx = id_index.build_from_lake(store)
    else:
        idx = id_index.load_or_build(store)
    _mark_running(store, mode)
    return idx


def _mark_running(store: ObjectStore, mode: str) -> None:
    marker = {"mode": mode, "started_at": datetime.now(UTC).isoformat()}
    store.put_bytes(RUN_MARKER_KEY, json.dumps(marker).encode("utf-8"))


def _finish(store: ObjectStore, idx: pd.DataFrame, result: RunResult) -> None:
    """Persiste índice, estado y manifest. Va al final: sólo tras escribir todo."""
    id_index.save(store, idx)
    state = load_state(store)
    # Conteos de todo el lake (incluye los años omitidos por ya existir).
    state.row_counts = id_index.row_counts(idx)
    # Informativo: la extracción no depende de él (ver secop.diff).
    top = id_index.max_watermark(idx)
    if top is not None:
        state.ultima_actualizacion = top.isoformat()
    state.schema_version = SCHEMA_VERSION
    save_state(store, state)
    _write_manifest(store, state)
    store.delete(RUN_MARKER_KEY)
    result.new_watermark = state.ultima_actualizacion


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
    """Avisa de contratos que el origen pasó de un año real a otro (se conservan ambos).

    Sólo cuenta los cambios nuevos: el contrato llega a un año donde el lake aún
    no lo tiene, y ya lo tiene en otro año real. Actualizar la copia de un
    contrato que ya estaba en dos años no es un cambio nuevo.
    """
    dated = idx[idx[id_index.COL_PARTITION] != lake.UNKNOWN_PARTITION]
    incoming = pd.DataFrame(
        {PRIMARY_KEY: df[PRIMARY_KEY].astype("string"), "target": targets}
    )
    incoming = incoming[incoming["target"] != lake.UNKNOWN_PARTITION]
    merged = incoming.merge(dated, on=PRIMARY_KEY)
    merged["already_there"] = merged["target"] == merged[id_index.COL_PARTITION]
    already = merged.groupby([PRIMARY_KEY, "target"])["already_there"].transform("any")
    changed = merged[~already]
    if not changed.empty:
        logger.warning(
            "%d contratos cambiaron de año de inicio en el origen; se conservan "
            "ambas copias (p.ej. %s)",
            changed[PRIMARY_KEY].nunique(),
            ", ".join(changed[PRIMARY_KEY].astype(str).unique()[:5]),
        )


def _apply_batch(
    store: ObjectStore, idx: pd.DataFrame, df: pd.DataFrame, detected_at: datetime
) -> tuple[pd.DataFrame, dict[str, int], int]:
    """Escribe un lote en el lake sin borrar nada.

    Devuelve (índice, filas totales por partición reescrita, contratos aplicados).
    """
    written: dict[str, int] = {}
    if df.empty or PRIMARY_KEY not in df.columns:
        return idx, written, 0
    batch = _classify(idx, df)
    applied = len(batch.to_write)
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
    # El índice (fiable gracias a la marca de corrida) evita leer year=unknown
    # cuando ningún id del lote está ahí.
    in_unknown = batch.dated_ids & set(
        idx.loc[idx[id_index.COL_PARTITION] == lake.UNKNOWN_PARTITION, PRIMARY_KEY]
    )
    if not in_unknown:
        return idx, written, applied
    remaining = lake.move_out_ids(
        store,
        lake.UNKNOWN_PARTITION,
        in_unknown,
        motivo=history.MOTIVO_FECHA_COMPLETADA,
        detected_at=detected_at,
    )
    if remaining is not None:
        idx = id_index.replace_partition(idx, lake.UNKNOWN_PARTITION, remaining)
        written[lake.UNKNOWN_PARTITION] = len(remaining)
    return idx, written, applied


def _record(result: RunResult, written: dict[str, int], applied: int = 0) -> None:
    # Una partición reescrita en varios lotes queda con su tamaño final.
    result.partitions.update(written)
    result.contracts_written += applied


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
    idx = _begin(store, "backfill")
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

        idx, written, applied = _apply_batch(store, idx, year_df, run_at)
        _record(result, written, applied)

    if only_year is None:
        logger.info(
            "Descargando registros fuera de rango (nulos / previos / futuros)..."
        )
        tail_df = _fetch_tail(start_year, end_year)
        idx, written, applied = _apply_batch(store, idx, tail_df, run_at)
        _record(result, written, applied)

    _finish(store, idx, result)
    return result


# --------------------------------------------------------------------------- #
# Incremental                                                                 #
# --------------------------------------------------------------------------- #
def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def incremental(store: ObjectStore) -> RunResult:
    """Pone el lake al día con el origen sin borrar nada.

    Baja la foto ligera del origen, la compara con el índice (``secop.diff``)
    y descarga completos sólo los ids nuevos, actualizados o fuera de su
    partición: contratos nuevos aunque no tengan ``ultima_actualizacion``,
    ``unknown`` que ya tienen fecha, cambios de año y versiones más nuevas.
    """
    result = RunResult(mode="incremental")
    idx = _begin(store, "incremental")
    run_at = datetime.now(UTC)

    snapshot = fetch_snapshot(diff.SNAPSHOT_COLUMNS)
    to_fetch = diff.ids_to_fetch(snapshot, idx)
    del snapshot
    for chunk in _chunks(to_fetch, FETCH_CHUNK_IDS):
        df = to_dataframe(fetch_by_ids(chunk))
        idx, written, applied = _apply_batch(store, idx, df, run_at)
        _record(result, written, applied)

    _finish(store, idx, result)
    return result


# --------------------------------------------------------------------------- #
# Reparación de datos escritos por versiones anteriores del ETL               #
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class RepairReport:
    """Lo que ``repair`` encontró (y, con ``apply``, corrigió)."""

    applied: bool
    #: Ids en year=unknown que además están en un año real (se mueven).
    unknown_with_date: int = 0
    #: Ids en dos o más años reales (evidencia: no se tocan).
    year_changes: int = 0
    backup_prefix: str | None = None
    result: RunResult = field(default_factory=lambda: RunResult(mode="repair"))


def _backup(store: ObjectStore, keys: list[str], at: datetime) -> str:
    """Copia (en el servidor) ``keys`` bajo ``backup/<timestamp>/``."""
    prefix = f"backup/{at.strftime('%Y%m%dT%H%M%SZ')}"
    for key in keys:
        if store.exists(key):
            store.copy(key, f"{prefix}/{key}")
            logger.info("Respaldo: %s -> %s/%s", key, prefix, key)
    return prefix


def repair(store: ObjectStore, *, apply: bool = False) -> RepairReport:
    """Saca de year=unknown los contratos que ya están en un año real.

    Sin ``apply`` sólo informa. Con ``apply`` respalda antes lo que va a tocar;
    las copias que salen de unknown quedan además en el historial.
    """
    idx = id_index.build_from_lake(store)
    part = idx[id_index.COL_PARTITION]
    unknown_ids = set(idx.loc[part == lake.UNKNOWN_PARTITION, PRIMARY_KEY])
    dated = idx[part != lake.UNKNOWN_PARTITION]
    dated_ids = set(dated[PRIMARY_KEY])
    report = RepairReport(
        applied=apply,
        unknown_with_date=len(unknown_ids & dated_ids),
        year_changes=int((dated.groupby(PRIMARY_KEY).size() > 1).sum()),
    )
    if not apply or not report.unknown_with_date:
        return report

    run_at = datetime.now(UTC)
    report.backup_prefix = _backup(
        store,
        [lake.data_key(lake.UNKNOWN_PARTITION), STATE_KEY, MANIFEST_KEY],
        run_at,
    )
    _mark_running(store, "repair")
    remaining = lake.move_out_ids(
        store,
        lake.UNKNOWN_PARTITION,
        unknown_ids & dated_ids,
        motivo=history.MOTIVO_FECHA_COMPLETADA,
        detected_at=run_at,
    )
    if remaining is not None:
        idx = id_index.replace_partition(idx, lake.UNKNOWN_PARTITION, remaining)
        _record(report.result, {lake.UNKNOWN_PARTITION: len(remaining)})
    _finish(store, idx, report.result)
    return report
