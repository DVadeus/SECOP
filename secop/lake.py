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
from datetime import UTC, datetime

import pandas as pd

from secop import schema
from secop.config import (
    LAKE_PREFIX,
    PARTITION_DATE_COLUMN,
    PRIMARY_KEY,
    WATERMARK_COLUMN,
)
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

UNKNOWN_PARTITION = "year=unknown"
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
    return f"{LAKE_PREFIX}/{partition}/data.parquet"


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
    mapping = {schema.BOOLEAN_TRUE: True, schema.BOOLEAN_FALSE: False}
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


def upsert_partition(store: ObjectStore, partition: str, new_rows: pd.DataFrame) -> int:
    """Fusiona ``new_rows`` con la partición existente (si la hay) y reescribe."""
    existing = read_partition(store, partition)
    if existing is None:
        combined = new_rows
    else:
        combined = pd.concat([existing, new_rows], ignore_index=True)
    return write_partition(store, partition, compact(combined))


def write_new_partition(store: ObjectStore, partition: str, rows: pd.DataFrame) -> int:
    """Escribe una partición desde cero (backfill), compactando dentro del lote."""
    return write_partition(store, partition, compact(rows.copy()))


def list_partitions(store: ObjectStore) -> list[str]:
    """Particiones presentes en el lake, en orden."""
    prefix = f"{LAKE_PREFIX}/"
    parts = {
        key[len(prefix) :].split("/", 1)[0]
        for key in store.list_keys(prefix)
        if key.endswith("data.parquet")
    }
    return sorted(parts)
