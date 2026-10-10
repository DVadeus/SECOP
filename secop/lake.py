"""Escritor del data lake Parquet: partición + compactación/upsert por clave.

Este módulo corrige el defecto central del ETL viejo: cada corrida incremental
añadía un ``part-<uuid>.parquet`` nuevo sin deduplicar, de modo que una misma
``id_contrato`` aparecía varias veces (versión vieja + nueva) al leer la
partición. Aquí cada partición es un único ``data.parquet`` compactado:
``drop_duplicates(id_contrato, keep=última ultima_actualizacion)``.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd

from secop import history, schema
from secop.config import (
    DATA_FILE,
    LAKE_PREFIX,
    PARTITION_DATE_COLUMN,
    PRIMARY_KEY,
    WATERMARK_COLUMN,
)
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

UNKNOWN_YEAR = "unknown"
UNKNOWN_PARTITION = f"year={UNKNOWN_YEAR}"
_MIN_REAL_YEAR = 2000


def _max_real_year() -> int:
    # Contratos que empiezan hasta ~2 años en el futuro son plausibles (vigencias
    # futuras). Más allá de eso es basura del origen -> year=unknown.
    return datetime.now(UTC).year + 2


def partition_for_year(year: int | None) -> str:
    """Nombre de partición Hive para un año de inicio de contrato."""
    if year is None or not (_MIN_REAL_YEAR <= year <= _max_real_year()):
        return UNKNOWN_PARTITION
    return f"year={year}"


def data_key(partition: str) -> str:
    """Clave del objeto Parquet de una partición."""
    return f"{LAKE_PREFIX}/{partition}/{DATA_FILE}"


def assign_partitions(df: pd.DataFrame) -> pd.Series:
    """Serie con la partición destino de cada fila (según fecha de inicio)."""
    if PARTITION_DATE_COLUMN not in df.columns:
        return pd.Series([UNKNOWN_PARTITION] * len(df), index=df.index)
    years = pd.to_datetime(df[PARTITION_DATE_COLUMN], errors="coerce").dt.year
    lo, hi = _MIN_REAL_YEAR, _max_real_year()
    valid = years.between(lo, hi)
    out = pd.Series(UNKNOWN_PARTITION, index=df.index, dtype="object")
    out[valid] = "year=" + years[valid].astype("Int64").astype(str)
    return out


def _canonicalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Reaplica el contrato de tipos de forma idempotente antes de escribir.

    Necesario porque ``read_parquet`` de una partición existente puede devolver
    dtypes distintos (``object`` en vez de ``boolean``, etc.) y el ``concat`` con
    el lote nuevo los mezcla. Las categóricas se dejan como ``string`` en el lake
    (pyarrow las codifica en diccionario igual).
    """
    for col in schema.DATE_COLUMNS:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in schema.NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    # Acepta tanto "Si"/"No" del origen como True/False ya convertidos (una
    # columna boolean concatenada con otra ausente puede quedar como object).
    mapping = {
        schema.BOOLEAN_TRUE: True,
        schema.BOOLEAN_FALSE: False,
        True: True,
        False: False,
    }
    for col in schema.BOOLEAN_COLUMNS:
        if col in df.columns and not pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].map(mapping).astype("boolean")
    for col in schema.CATEGORICAL_COLUMNS:
        if col in df.columns and isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype("object")
    return df


def compact(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplica por ``id_contrato`` conservando la ``ultima_actualizacion`` mayor."""
    if df.empty or PRIMARY_KEY not in df.columns:
        return df.reset_index(drop=True)
    if WATERMARK_COLUMN in df.columns:
        df = df.sort_values(WATERMARK_COLUMN, na_position="first", kind="stable")
    deduped = df.drop_duplicates(subset=[PRIMARY_KEY], keep="last")
    return deduped.reset_index(drop=True)


@dataclass(slots=True)
class MergeResult:
    """Resultado de fusionar versiones de contratos en una partición."""

    #: Una fila por ``id_contrato``: la versión vigente.
    rows: pd.DataFrame
    #: Versiones perdedoras cuyo contenido difiere de la vigente (van al historial).
    superseded: pd.DataFrame


def merge_versions(
    existing: pd.DataFrame | None, incoming: pd.DataFrame
) -> MergeResult:
    """Fusiona ``incoming`` sobre ``existing`` quedándose con una versión por id.

    Gana la ``ultima_actualizacion`` mayor; en empate (o ambas nulas) gana la
    más reciente en llegar (``incoming``). Las perdedoras con contenido distinto
    de la ganadora se devuelven en ``superseded`` para no perderlas.
    """
    frames = [
        _canonicalize_dtypes(f.copy())
        for f in (existing, incoming)
        if f is not None and not f.empty
    ]
    if not frames:
        empty = incoming.iloc[0:0]
        return MergeResult(rows=empty, superseded=empty)
    combined = pd.concat(frames, ignore_index=True)
    if PRIMARY_KEY not in combined.columns:
        return MergeResult(rows=combined, superseded=combined.iloc[0:0])

    if WATERMARK_COLUMN in combined.columns:
        combined = combined.sort_values(
            WATERMARK_COLUMN, na_position="first", kind="stable"
        )
    winner = ~combined.duplicated(subset=[PRIMARY_KEY], keep="last")
    superseded = _changed_losers(combined, winner)
    return MergeResult(
        rows=combined[winner].reset_index(drop=True),
        superseded=superseded.reset_index(drop=True),
    )


def _changed_losers(combined: pd.DataFrame, winner: pd.Series) -> pd.DataFrame:
    """Perdedoras cuyo contenido difiere del de la ganadora de su mismo id."""
    if winner.all():
        return combined.iloc[0:0]
    # Sólo se hashean los ids con más de una versión (evita hashear la
    # partición entera en cada upsert).
    contested = combined[combined[PRIMARY_KEY].duplicated(keep=False)]
    hashes = history.content_hash(contested)
    is_winner = winner.loc[contested.index]
    winner_hash = pd.Series(
        hashes[is_winner].to_numpy(),
        index=contested.loc[is_winner, PRIMARY_KEY].to_numpy(),
    )
    losers = contested[~is_winner]
    loser_hash = hashes[~is_winner]
    differs = loser_hash.to_numpy() != losers[PRIMARY_KEY].map(winner_hash).to_numpy()
    changed = losers[differs]
    # Varias perdedoras idénticas entre sí sólo cuentan una vez.
    return changed[~loser_hash[differs].duplicated().to_numpy()]


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    return buffer.getvalue()


def read_partition(store: ObjectStore, partition: str) -> pd.DataFrame | None:
    """Lee el ``data.parquet`` de una partición, o ``None`` si no existe."""
    try:
        raw = store.get_bytes(data_key(partition))
    except KeyError:
        return None
    return pd.read_parquet(io.BytesIO(raw))


def write_partition(store: ObjectStore, partition: str, df: pd.DataFrame) -> int:
    """Escribe (sobrescribe) la partición con ``df`` ya compactado. Devuelve nº filas."""
    df = _canonicalize_dtypes(df.copy())
    store.put_bytes(data_key(partition), _to_parquet_bytes(df))
    logger.info("Partición %s escrita: %d filas (%s)", partition, len(df), store.label)
    return len(df)


def _apply_merge(
    store: ObjectStore,
    partition: str,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    detected_at: datetime | None,
) -> pd.DataFrame:
    result = merge_versions(existing, incoming)
    # Historial ANTES que la partición: si la escritura de la partición falla,
    # la versión reemplazada ya está a salvo (y append es idempotente).
    history.append(
        store,
        result.superseded,
        partition=partition,
        motivo=history.MOTIVO_VERSION,
        detected_at=detected_at,
    )
    write_partition(store, partition, result.rows)
    return result.rows


def upsert_partition(
    store: ObjectStore,
    partition: str,
    new_rows: pd.DataFrame,
    *,
    detected_at: datetime | None = None,
) -> pd.DataFrame:
    """Fusiona ``new_rows`` con la partición existente (si la hay) y reescribe.

    Nunca elimina contratos: los ids que no vienen en ``new_rows`` se conservan.
    Devuelve el contenido escrito de la partición.
    """
    existing = read_partition(store, partition)
    return _apply_merge(store, partition, existing, new_rows, detected_at)


def write_new_partition(
    store: ObjectStore,
    partition: str,
    rows: pd.DataFrame,
    *,
    detected_at: datetime | None = None,
) -> pd.DataFrame:
    """Escribe una partición desde cero (sólo tests/partición inexistente)."""
    return _apply_merge(store, partition, None, rows, detected_at)


def move_out_ids(
    store: ObjectStore,
    partition: str,
    ids: set[str],
    *,
    motivo: str,
    detected_at: datetime | None = None,
) -> pd.DataFrame | None:
    """Saca ``ids`` de ``partition`` guardándolos antes en el historial.

    Se usa cuando una versión más completa del contrato ya quedó escrita en
    otra partición (``year=unknown`` -> año real). Lee la partición real, así
    que no depende de que el índice esté al día. Devuelve el contenido escrito,
    o ``None`` si no había nada que mover.
    """
    existing = read_partition(store, partition)
    if existing is None or not ids or PRIMARY_KEY not in existing.columns:
        return None
    leaving = existing[PRIMARY_KEY].isin(ids)
    if not leaving.any():
        return None
    history.append(
        store,
        existing[leaving],
        partition=partition,
        motivo=motivo,
        detected_at=detected_at,
    )
    remaining = existing[~leaving].reset_index(drop=True)
    write_partition(store, partition, remaining)
    logger.info("%s: %d contratos movidos (%s)", partition, int(leaving.sum()), motivo)
    return remaining


def list_partitions(store: ObjectStore) -> list[str]:
    """Particiones presentes en el lake, en orden."""
    prefix = f"{LAKE_PREFIX}/"
    parts = {
        key[len(prefix) :].split("/", 1)[0]
        for key in store.list_keys(prefix)
        if key.endswith(f"/{DATA_FILE}")
    }
    return sorted(parts)
