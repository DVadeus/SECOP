"""Comparación entre la foto ligera del origen y el índice del lake.

``ultima_actualizacion`` es nula en ~38 % del origen, así que no basta para
detectar novedades. Aquí se decide qué ``id_contrato`` hay que descargar
completos comparando, para cada fila del origen, su partición destino y su
versión con lo que el índice dice que ya hay en el lake. No hay watermark
global: cada id se compara con su propia versión, así que ningún cambio se
pierde por un watermark adelantado (p.ej. tras un backfill reanudado).
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

from secop import lake
from secop.bases import CONTRATOS, Base
from secop.index import COL_ID, COL_PARTITION, COL_VERSION

logger = logging.getLogger(__name__)


def snapshot_columns(base: Base = CONTRATOS) -> tuple[str, ...]:
    """Columnas de la foto ligera de ``base``: clave, versión y fecha de partición."""
    if base.version_column is None:
        raise ValueError(f"{base.name} no tiene columna de versión para la foto")
    return (base.key, base.version_column, base.partition_date_column)


#: Columnas de la foto ligera de la base de contratos.
SNAPSHOT_COLUMNS = snapshot_columns(CONTRATOS)


def _prepare_snapshot(snapshot: pd.DataFrame, base: Base) -> pd.DataFrame:
    # Una columna ausente en todas las páginas (p.ej. todo nulo) cuenta como nula.
    key, version, _ = snapshot_columns(base)
    snapshot = snapshot.reindex(columns=list(snapshot_columns(base)))
    out = pd.DataFrame(
        {
            COL_ID: snapshot[key].astype("string"),
            "target": lake.assign_partitions(snapshot, base).astype("string"),
            "ua": pd.to_datetime(snapshot[version], errors="coerce"),
        }
    )
    return out.dropna(subset=[COL_ID])


# Ids del origen que hay que descargar:
#
# * no está en su partición destino (id nuevo, unknown -> año real, cambio de
#   año): siempre;
# * destino unknown pero el lake ya lo tiene con fecha (el origen borró la
#   fecha): sólo si trae una ultima_actualizacion más nueva que la del lake;
#   si no, ya se registró en el historial y no hay nada nuevo que bajar;
# * ya está en su partición pero el origen trae una ultima_actualizacion más
#   nueva (o el lake la tiene nula).
_FETCH_SQL = f"""
WITH missing AS (
    SELECT s.{COL_ID}, s.target, s.ua
    FROM snap s
    ANTI JOIN idx i
      ON s.{COL_ID} = i.{COL_ID} AND s.target = i.{COL_PARTITION}
),
dated AS (
    SELECT {COL_ID}, max({COL_VERSION}) AS ua_max
    FROM idx
    WHERE {COL_PARTITION} <> '{lake.UNKNOWN_PARTITION}'
    GROUP BY 1
),
updated AS (
    SELECT s.{COL_ID}
    FROM snap s
    JOIN idx i
      ON s.{COL_ID} = i.{COL_ID} AND s.target = i.{COL_PARTITION}
    WHERE s.ua IS NOT NULL
      AND (i.{COL_VERSION} IS NULL OR s.ua > i.{COL_VERSION})
)
SELECT m.{COL_ID}
FROM missing m
LEFT JOIN dated d USING ({COL_ID})
WHERE m.target <> '{lake.UNKNOWN_PARTITION}'
   OR d.{COL_ID} IS NULL
   OR (m.ua IS NOT NULL AND (d.ua_max IS NULL OR m.ua > d.ua_max))
UNION
SELECT {COL_ID} FROM updated
ORDER BY 1
"""


def ids_to_fetch(
    snapshot: pd.DataFrame, idx: pd.DataFrame, base: Base = CONTRATOS
) -> list[str]:
    """Ids del origen que hay que descargar completos para poner el lake al día."""
    snap = _prepare_snapshot(snapshot, base)
    con = duckdb.connect(database=":memory:")
    try:
        con.register("snap", snap)
        con.register("idx", idx)
        rows = con.execute(_FETCH_SQL).fetchall()
    finally:
        con.close()
    ids = [str(r[0]) for r in rows]
    logger.info(
        "Foto del origen: %d filas; %d ids nuevos, actualizados o fuera de "
        "su partición",
        len(snap),
        len(ids),
    )
    return ids
