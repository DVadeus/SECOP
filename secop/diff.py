"""Comparación entre la foto ligera del origen y el índice del lake.

``ultima_actualizacion`` es nula en ~38 % del origen, así que no basta para
detectar novedades. Aquí se decide qué ``id_contrato`` hay que descargar
completos comparando, para cada fila del origen, su partición destino con lo
que el índice dice que ya hay en el lake.
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

from secop import lake
from secop.config import PARTITION_DATE_COLUMN, PRIMARY_KEY, WATERMARK_COLUMN
from secop.index import COL_PARTITION

logger = logging.getLogger(__name__)

#: Columnas de la foto ligera que se descarga del origen.
SNAPSHOT_COLUMNS = (PRIMARY_KEY, WATERMARK_COLUMN, PARTITION_DATE_COLUMN)


def _prepare_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            PRIMARY_KEY: snapshot[PRIMARY_KEY].astype("string"),
            "target": lake.assign_partitions(snapshot).astype("string"),
            "ua": pd.to_datetime(snapshot[WATERMARK_COLUMN], errors="coerce"),
        }
    )
    return out.dropna(subset=[PRIMARY_KEY])


# Ids del origen que no están en su partición destino.
#
# * id nuevo, unknown -> año real, cambio de año: siempre.
# * destino unknown pero el lake ya lo tiene con fecha (el origen borró la
#   fecha): sólo si trae una ultima_actualizacion más nueva que la del lake;
#   si no, ya se registró en el historial y no hay nada nuevo que bajar.
_MISSING_SQL = f"""
WITH missing AS (
    SELECT s.{PRIMARY_KEY}, s.target, s.ua
    FROM snap s
    ANTI JOIN idx i
      ON s.{PRIMARY_KEY} = i.{PRIMARY_KEY} AND s.target = i.{COL_PARTITION}
),
dated AS (
    SELECT {PRIMARY_KEY}, max({WATERMARK_COLUMN}) AS ua_max
    FROM idx
    WHERE {COL_PARTITION} <> '{lake.UNKNOWN_PARTITION}'
    GROUP BY 1
)
SELECT DISTINCT m.{PRIMARY_KEY}
FROM missing m
LEFT JOIN dated d USING ({PRIMARY_KEY})
WHERE m.target <> '{lake.UNKNOWN_PARTITION}'
   OR d.{PRIMARY_KEY} IS NULL
   OR (m.ua IS NOT NULL AND (d.ua_max IS NULL OR m.ua > d.ua_max))
ORDER BY 1
"""


def ids_to_fetch(snapshot: pd.DataFrame, idx: pd.DataFrame) -> list[str]:
    """Ids del origen que hay que descargar completos para poner el lake al día."""
    snap = _prepare_snapshot(snapshot)
    con = duckdb.connect(database=":memory:")
    try:
        con.register("snap", snap)
        con.register("idx", idx)
        rows = con.execute(_MISSING_SQL).fetchall()
    finally:
        con.close()
    ids = [str(r[0]) for r in rows]
    logger.info(
        "Foto del origen: %d filas; %d ids nuevos o fuera de su partición",
        len(snap),
        len(ids),
    )
    return ids
