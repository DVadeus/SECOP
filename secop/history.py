"""Historial de versiones reemplazadas de los contratos.

El lake principal guarda la versión vigente de cada ``id_contrato``. Cuando una
versión es reemplazada por otra con contenido distinto, la anterior no se
pierde: se guarda aquí, en ``raw/contratos_historial/detectado=YYYY-MM-DD/``,
con el motivo, la partición de origen (columna ``year``) y la hora de
detección. El historial es sólo de anexión: nunca se borran filas.
"""

from __future__ import annotations

import io
import logging
from datetime import UTC, datetime

import pandas as pd

from secop.bases import CONTRATOS, Base
from secop.storage.base import ObjectStore

logger = logging.getLogger(__name__)

#: Otra versión del mismo contrato, en la misma partición, la reemplazó.
MOTIVO_VERSION = "version_reemplazada"
#: Estaba en year=unknown y llegó una versión con fecha de inicio: se movió.
MOTIVO_FECHA_COMPLETADA = "fecha_completada"
#: El origen quitó la fecha de inicio a un contrato que ya la tenía en el lake;
#: la versión con fecha se conserva y la nueva (sin fecha) queda aquí.
MOTIVO_FECHA_ELIMINADA = "fecha_eliminada_en_origen"

COL_MOTIVO = "historial_motivo"
COL_DETECTADO = "historial_detectado_utc"
COL_HASH = "historial_hash"
COL_YEAR = "year"
_DEDUP_KEY = [COL_HASH, COL_YEAR, COL_MOTIVO]


def history_key(detected_at: datetime, base: Base = CONTRATOS) -> str:
    """Clave del archivo de historial de ``base`` del día de ``detected_at``."""
    day = detected_at.astimezone(UTC).date().isoformat()
    return f"{base.history_prefix}/detectado={day}/{base.data_file}"


def content_hash(df: pd.DataFrame) -> pd.Series:
    """Hash por fila del contenido, independiente del orden de columnas.

    Las columnas de texto se normalizan a ``string`` para que ``None`` (leído de
    Parquet) y ``NaN`` (recién transformado) cuenten como el mismo nulo.
    """
    cols = sorted(c for c in df.columns if not str(c).startswith("historial_"))
    normalized = df[cols].copy()
    for col in cols:
        if normalized[col].dtype == object:
            normalized[col] = normalized[col].astype("string")
    return pd.util.hash_pandas_object(normalized, index=False)


def _read(store: ObjectStore, key: str) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(io.BytesIO(store.get_bytes(key)))
    except KeyError:
        return None


def append(
    store: ObjectStore,
    rows: pd.DataFrame,
    *,
    partition: str,
    motivo: str,
    detected_at: datetime | None = None,
    base: Base = CONTRATOS,
) -> int:
    """Anexa ``rows`` al historial del día. Devuelve cuántas filas nuevas quedaron.

    Idempotente: una fila con el mismo contenido, partición y motivo ya
    registrada ese día no se duplica (p.ej. al reintentar una corrida fallida).
    """
    if rows.empty:
        return 0
    detected_at = detected_at or datetime.now(UTC)
    batch = rows.copy()
    batch[COL_HASH] = content_hash(batch).astype("string")
    batch[COL_YEAR] = partition.removeprefix("year=")
    batch[COL_MOTIVO] = motivo
    batch[COL_DETECTADO] = pd.Timestamp(detected_at.astimezone(UTC)).tz_localize(None)

    key = history_key(detected_at, base)
    existing = _read(store, key)
    before = 0 if existing is None else len(existing)
    combined = batch if existing is None else pd.concat([existing, batch])
    combined = combined.drop_duplicates(subset=_DEDUP_KEY, keep="first")
    added = len(combined) - before
    if added == 0:
        return 0

    buffer = io.BytesIO()
    combined.reset_index(drop=True).to_parquet(
        buffer, index=False, compression="snappy"
    )
    store.put_bytes(key, buffer.getvalue())
    logger.info("Historial %s: +%d versiones (%s, %s)", key, added, partition, motivo)
    return added
