"""Índice de ids del lake: una fila por (``id_contrato``, partición).

Permite saber en qué partición(es) está cada contrato y con qué
``ultima_actualizacion`` sin descargar las particiones completas. Es un dato
derivado: si falta o se desincroniza, se reconstruye leyendo el lake
(``build_from_lake``). Nunca se usa para decidir qué borrar.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import pyarrow.parquet as pq

from secop import lake
from secop.config import INDEX_KEY, PRIMARY_KEY, WATERMARK_COLUMN
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

COL_PARTITION = "partition"
COLUMNS = [PRIMARY_KEY, COL_PARTITION, WATERMARK_COLUMN]


def empty() -> pd.DataFrame:
    """Índice vacío con el esquema correcto."""
    return pd.DataFrame(
        {
            PRIMARY_KEY: pd.Series(dtype="string"),
            COL_PARTITION: pd.Series(dtype="string"),
            WATERMARK_COLUMN: pd.Series(dtype="datetime64[ns]"),
        }
    )


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df[COLUMNS].copy()
    out[PRIMARY_KEY] = out[PRIMARY_KEY].astype("string")
    out[COL_PARTITION] = out[COL_PARTITION].astype("string")
    out[WATERMARK_COLUMN] = pd.to_datetime(
        out[WATERMARK_COLUMN], errors="coerce"
    ).astype("datetime64[ns]")
    return out.reset_index(drop=True)


def entries_for(partition: str, rows: pd.DataFrame) -> pd.DataFrame:
    """Entradas de índice de las filas ``rows`` de ``partition``."""
    if rows.empty or PRIMARY_KEY not in rows.columns:
        return empty()
    ua = (
        rows[WATERMARK_COLUMN]
        if WATERMARK_COLUMN in rows.columns
        else pd.Series(pd.NaT, index=rows.index)
    )
    return _normalize(
        pd.DataFrame(
            {
                PRIMARY_KEY: rows[PRIMARY_KEY],
                COL_PARTITION: partition,
                WATERMARK_COLUMN: ua,
            }
        )
    )


def replace_partition(
    index: pd.DataFrame, partition: str, rows: pd.DataFrame
) -> pd.DataFrame:
    """Devuelve el índice con las entradas de ``partition`` sustituidas por ``rows``."""
    kept = index[index[COL_PARTITION] != partition]
    return pd.concat([kept, entries_for(partition, rows)], ignore_index=True)


def row_counts(index: pd.DataFrame) -> dict[str, int]:
    """Filas por partición de TODO el lake (no sólo lo tocado en la corrida)."""
    counts = index.groupby(COL_PARTITION).size()
    return {str(p): int(n) for p, n in sorted(counts.items())}


def load(store: ObjectStore) -> pd.DataFrame | None:
    """Carga el índice, o ``None`` si aún no existe."""
    try:
        raw = store.get_bytes(INDEX_KEY)
    except KeyError:
        return None
    return _normalize(pd.read_parquet(io.BytesIO(raw)))


def save(store: ObjectStore, index: pd.DataFrame) -> None:
    buffer = io.BytesIO()
    _normalize(index).to_parquet(buffer, index=False, compression="snappy")
    store.put_bytes(INDEX_KEY, buffer.getvalue())
    logger.info("Índice guardado: %d entradas", len(index))


def build_from_lake(store: ObjectStore) -> pd.DataFrame:
    """Reconstruye el índice leyendo ``id_contrato`` y la versión de cada partición."""
    frames = [empty()]
    for partition in lake.list_partitions(store):
        raw = store.get_bytes(lake.data_key(partition))
        cols = pq.ParquetFile(io.BytesIO(raw)).schema_arrow.names
        wanted = [c for c in (PRIMARY_KEY, WATERMARK_COLUMN) if c in cols]
        rows = pd.read_parquet(io.BytesIO(raw), columns=wanted)
        frames.append(entries_for(partition, rows))
        logger.info("Índice: %s -> %d ids", partition, len(rows))
    return pd.concat(frames, ignore_index=True)


def load_or_build(store: ObjectStore) -> pd.DataFrame:
    """Carga el índice; si no existe, lo reconstruye desde el lake."""
    loaded = load(store)
    if loaded is not None:
        return loaded
    logger.warning(
        "No hay índice de ids en %s; se reconstruye desde el lake", store.label
    )
    return build_from_lake(store)
