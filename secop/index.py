"""Índice de ids del lake: una fila por (``id_contrato``, partición).

Permite saber en qué partición(es) está cada contrato y con qué
``ultima_actualizacion`` sin descargar las particiones completas. Es un dato
derivado: si falta o se desincroniza, se reconstruye leyendo el lake
(``build_from_lake``). Nunca se usa para decidir qué borrar.

Cada base tiene su propio índice (``base.index_key``). Las columnas del índice
tienen nombres fijos; al construirlo se leen la clave y la versión de la base.
"""

from __future__ import annotations

import io
import logging
from datetime import UTC, datetime

import pandas as pd
import pyarrow.parquet as pq

from secop import lake
from secop.bases import CONTRATOS, Base
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

COL_ID = "id_contrato"
COL_PARTITION = "partition"
COL_VERSION = "ultima_actualizacion"
COLUMNS = [COL_ID, COL_PARTITION, COL_VERSION]


def empty() -> pd.DataFrame:
    """Índice vacío con el esquema correcto."""
    return pd.DataFrame(
        {
            COL_ID: pd.Series(dtype="string"),
            COL_PARTITION: pd.Series(dtype="string"),
            COL_VERSION: pd.Series(dtype="datetime64[ns]"),
        }
    )


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df[COLUMNS].copy()
    out[COL_ID] = out[COL_ID].astype("string")
    out[COL_PARTITION] = out[COL_PARTITION].astype("string")
    out[COL_VERSION] = pd.to_datetime(out[COL_VERSION], errors="coerce").astype(
        "datetime64[ns]"
    )
    return out.reset_index(drop=True)


def entries_for(
    partition: str, rows: pd.DataFrame, base: Base = CONTRATOS
) -> pd.DataFrame:
    """Entradas de índice de las filas ``rows`` de ``partition``."""
    if rows.empty or base.key not in rows.columns:
        return empty()
    version = base.version_column
    ua = (
        rows[version]
        if version and version in rows.columns
        else pd.Series(pd.NaT, index=rows.index)
    )
    return _normalize(
        pd.DataFrame(
            {COL_ID: rows[base.key], COL_PARTITION: partition, COL_VERSION: ua}
        )
    )


def replace_partition(
    index: pd.DataFrame, partition: str, rows: pd.DataFrame, base: Base = CONTRATOS
) -> pd.DataFrame:
    """Devuelve el índice con las entradas de ``partition`` sustituidas por ``rows``."""
    kept = index[index[COL_PARTITION] != partition]
    return pd.concat([kept, entries_for(partition, rows, base)], ignore_index=True)


def row_counts(index: pd.DataFrame) -> dict[str, int]:
    """Filas por partición de TODO el lake (no sólo lo tocado en la corrida)."""
    counts = index.groupby(COL_PARTITION).size()
    return {str(p): int(n) for p, n in sorted(counts.items())}


def max_watermark(index: pd.DataFrame) -> datetime | None:
    """Mayor ``ultima_actualizacion`` presente en el lake (informativo)."""
    top = index[COL_VERSION].max() if not index.empty else pd.NaT
    if pd.isna(top):
        return None
    return pd.Timestamp(top).to_pydatetime().replace(tzinfo=UTC)


def load(store: ObjectStore, base: Base = CONTRATOS) -> pd.DataFrame | None:
    """Carga el índice de ``base``, o ``None`` si aún no existe."""
    try:
        raw = store.get_bytes(base.index_key)
    except KeyError:
        return None
    return _normalize(pd.read_parquet(io.BytesIO(raw)))


def save(store: ObjectStore, index: pd.DataFrame, base: Base = CONTRATOS) -> None:
    buffer = io.BytesIO()
    _normalize(index).to_parquet(buffer, index=False, compression="snappy")
    store.put_bytes(base.index_key, buffer.getvalue())
    logger.info("Índice de %s guardado: %d entradas", base.name, len(index))


def build_from_lake(store: ObjectStore, base: Base = CONTRATOS) -> pd.DataFrame:
    """Reconstruye el índice leyendo la clave y la versión de cada partición."""
    frames = [empty()]
    for partition in lake.list_partitions(store, base):
        raw = store.get_bytes(lake.data_key(partition, base))
        cols = pq.ParquetFile(io.BytesIO(raw)).schema_arrow.names
        wanted = [c for c in (base.key, base.version_column) if c and c in cols]
        rows = pd.read_parquet(io.BytesIO(raw), columns=wanted)
        frames.append(entries_for(partition, rows, base))
        logger.info("Índice de %s: %s -> %d filas", base.name, partition, len(rows))
    return pd.concat(frames, ignore_index=True)


def load_or_build(store: ObjectStore, base: Base = CONTRATOS) -> pd.DataFrame:
    """Carga el índice; si no existe, lo reconstruye desde el lake."""
    loaded = load(store, base)
    if loaded is not None:
        return loaded
    logger.warning(
        "No hay índice de %s en %s; se reconstruye desde el lake",
        base.name,
        store.label,
    )
    return build_from_lake(store, base)
