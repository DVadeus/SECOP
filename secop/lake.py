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
from secop.bases import CONTRATOS, Base, SchemaSpec
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


def data_key(partition: str, base: Base = CONTRATOS) -> str:
    """Clave del objeto Parquet de una partición."""
    return f"{base.lake_prefix}/{partition}/{base.data_file}"


def assign_partitions(df: pd.DataFrame, base: Base = CONTRATOS) -> pd.Series:
    """Serie con la partición destino de cada fila (según fecha de inicio).

    En una base agrupada todas las filas de un contrato van a la misma
    partición: la del año válido más temprano entre sus filas (``unknown`` si
    ninguna tiene fecha válida), para que el contrato nunca se parta.
    """
    if base.partition_date_column not in df.columns:
        return pd.Series([UNKNOWN_PARTITION] * len(df), index=df.index)
    dates = df[base.partition_date_column]
    years = pd.to_datetime(dates, errors="coerce").dt.year
    lo, hi = _MIN_REAL_YEAR, _max_real_year()
    valid = years.between(lo, hi)
    if base.grouped and base.key in df.columns:
        years = years.where(valid).groupby(df[base.key]).transform("min")
        valid = years.notna()
    out = pd.Series(UNKNOWN_PARTITION, index=df.index, dtype="object")
    out[valid] = "year=" + years[valid].astype("Int64").astype(str)
    return out


def _canonicalize_dtypes(
    df: pd.DataFrame, spec: SchemaSpec = CONTRATOS.schema
) -> pd.DataFrame:
    """Reaplica el contrato de tipos de forma idempotente antes de escribir.

    Necesario porque ``read_parquet`` de una partición existente puede devolver
    dtypes distintos (``object`` en vez de ``boolean``, etc.) y el ``concat`` con
    el lote nuevo los mezcla. Las categóricas se dejan como ``string`` en el lake
    (pyarrow las codifica en diccionario igual).
    """
    for col in spec.date_columns:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in spec.numeric_columns:
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
    for col in spec.boolean_columns:
        if col in df.columns and not pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].map(mapping).astype("boolean")
    for col in spec.categorical_columns:
        if col in df.columns and isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype("object")
    return df


def _sort_by_version(df: pd.DataFrame, base: Base) -> pd.DataFrame:
    """Ordena de versión más vieja a más nueva (estable; nulas primero)."""
    if base.version_column and base.version_column in df.columns:
        return df.sort_values(base.version_column, na_position="first", kind="stable")
    return df


def compact(df: pd.DataFrame, base: Base = CONTRATOS) -> pd.DataFrame:
    """Deduplica por ``id_contrato`` conservando la ``ultima_actualizacion`` mayor."""
    if df.empty or base.key not in df.columns:
        return df.reset_index(drop=True)
    df = _sort_by_version(df, base)
    deduped = df.drop_duplicates(subset=[base.key], keep="last")
    return deduped.reset_index(drop=True)


@dataclass(slots=True)
class MergeResult:
    """Resultado de fusionar versiones de contratos en una partición."""

    #: Versión vigente de cada contrato (una fila, o varias en bases agrupadas).
    rows: pd.DataFrame
    #: Versiones perdedoras cuyo contenido difiere de la vigente (van al historial).
    superseded: pd.DataFrame
    #: ``False`` si la partición quedaría idéntica (no hace falta reescribirla).
    changed: bool = True


def merge_versions(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    base: Base = CONTRATOS,
) -> MergeResult:
    """Fusiona ``incoming`` sobre ``existing`` quedándose con una versión por id.

    Gana la ``ultima_actualizacion`` mayor; en empate (o ambas nulas) gana la
    más reciente en llegar (``incoming``). Las perdedoras con contenido distinto
    de la ganadora se devuelven en ``superseded`` para no perderlas. En una
    base agrupada la versión es el contrato completo (ver ``_merge_groups``).
    """
    if base.grouped:
        return _merge_groups(existing, incoming, base)
    frames = [
        _canonicalize_dtypes(f.copy(), base.schema)
        for f in (existing, incoming)
        if f is not None and not f.empty
    ]
    if not frames:
        empty = incoming.iloc[0:0]
        return MergeResult(rows=empty, superseded=empty)
    combined = pd.concat(frames, ignore_index=True)
    if base.key not in combined.columns:
        return MergeResult(rows=combined, superseded=combined.iloc[0:0])

    combined = _sort_by_version(combined, base)
    winner = ~combined.duplicated(subset=[base.key], keep="last")
    superseded = _changed_losers(combined, winner, base.key)
    return MergeResult(
        rows=combined[winner].reset_index(drop=True),
        superseded=superseded.reset_index(drop=True),
    )


def _signatures(keys: pd.Series, hashes: pd.Series) -> pd.Series:
    """Firma de cada contrato: sus hashes de fila ordenados (multiconjunto)."""
    frame = pd.DataFrame({"key": keys.to_numpy(), "h": hashes.to_numpy()})
    return frame.groupby("key")["h"].agg(lambda s: tuple(sorted(s)))


def _merge_groups(
    existing: pd.DataFrame | None, incoming: pd.DataFrame, base: Base
) -> MergeResult:
    """Merge de una base agrupada: la unidad de versión es el contrato completo.

    Las filas idénticas de un mismo contrato se colapsan. Si las filas de un
    contrato no cambiaron se conserva tal cual; si cambió cualquiera, todas las
    filas anteriores van a ``superseded`` y entran las nuevas. Los contratos
    que no vienen en ``incoming`` se conservan (nunca se borra).
    """
    key = base.key
    frames = [
        _canonicalize_dtypes(f.copy(), base.schema).assign(_origen=n)
        for n, f in enumerate((existing, incoming))
        if f is not None and not f.empty
    ]
    if not frames:
        empty = incoming.iloc[0:0]
        return MergeResult(rows=empty, superseded=empty, changed=False)
    # Hash sobre el concat: ambos lados con las mismas columnas y dtypes.
    combined = pd.concat(frames, ignore_index=True)
    hashes = history.content_hash(combined.drop(columns="_origen"))
    is_new = combined["_origen"] == 1
    # Filas idénticas sólo se colapsan dentro de lo que llega: una fila nueva
    # igual a una existente sigue siendo parte del contrato nuevo.
    dup_new = pd.Series(False, index=combined.index)
    dup_new[is_new] = hashes[is_new].duplicated().to_numpy()
    unique_new = is_new & ~dup_new
    sig_new = _signatures(combined.loc[unique_new, key], hashes[unique_new])
    old_mask = ~is_new & combined[key].isin(sig_new.index)
    sig_old = _signatures(combined.loc[old_mask, key], hashes[old_mask])
    same = sig_old.reindex(sig_new.index) == sig_new
    replace = set(sig_new.index[~same])
    superseded = combined[~is_new & combined[key].isin(replace)]
    rows = combined[
        (~is_new & ~combined[key].isin(replace))
        | (unique_new & combined[key].isin(replace))
    ]
    return MergeResult(
        rows=rows.drop(columns="_origen").reset_index(drop=True),
        superseded=superseded.drop(columns="_origen").reset_index(drop=True),
        # Los contratos nuevos también están en ``replace`` (sin firma vieja).
        changed=bool(replace),
    )


def _changed_losers(
    combined: pd.DataFrame, winner: pd.Series, key: str
) -> pd.DataFrame:
    """Perdedoras cuyo contenido difiere del de la ganadora de su mismo id."""
    if winner.all():
        return combined.iloc[0:0]
    # Sólo se hashean los ids con más de una versión (evita hashear la
    # partición entera en cada upsert).
    contested = combined[combined[key].duplicated(keep=False)]
    hashes = history.content_hash(contested)
    is_winner = winner.loc[contested.index]
    winner_hash = pd.Series(
        hashes[is_winner].to_numpy(),
        index=contested.loc[is_winner, key].to_numpy(),
    )
    losers = contested[~is_winner]
    loser_hash = hashes[~is_winner]
    differs = loser_hash.to_numpy() != losers[key].map(winner_hash).to_numpy()
    changed = losers[differs]
    # Varias perdedoras idénticas entre sí sólo cuentan una vez.
    return changed[~loser_hash[differs].duplicated().to_numpy()]


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    return buffer.getvalue()


def read_partition(
    store: ObjectStore, partition: str, base: Base = CONTRATOS
) -> pd.DataFrame | None:
    """Lee el archivo de ``base`` en una partición, o ``None`` si no existe."""
    try:
        raw = store.get_bytes(data_key(partition, base))
    except KeyError:
        return None
    return pd.read_parquet(io.BytesIO(raw))


def write_partition(
    store: ObjectStore, partition: str, df: pd.DataFrame, base: Base = CONTRATOS
) -> int:
    """Escribe (sobrescribe) la partición con ``df`` ya compactado. Devuelve nº filas."""
    df = _canonicalize_dtypes(df.copy(), base.schema)
    store.put_bytes(data_key(partition, base), _to_parquet_bytes(df))
    logger.info(
        "Partición %s/%s escrita: %d filas (%s)",
        partition,
        base.data_file,
        len(df),
        store.label,
    )
    return len(df)


def _apply_merge(
    store: ObjectStore,
    partition: str,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    detected_at: datetime | None,
    base: Base,
) -> pd.DataFrame:
    result = merge_versions(existing, incoming, base)
    # Historial ANTES que la partición: si la escritura de la partición falla,
    # la versión reemplazada ya está a salvo (y append es idempotente).
    history.append(
        store,
        result.superseded,
        partition=partition,
        motivo=history.MOTIVO_VERSION,
        detected_at=detected_at,
        base=base,
    )
    if result.changed:
        write_partition(store, partition, result.rows, base)
    else:
        logger.info("Partición %s/%s sin cambios", partition, base.data_file)
    return result.rows


def upsert_partition(
    store: ObjectStore,
    partition: str,
    new_rows: pd.DataFrame,
    *,
    detected_at: datetime | None = None,
    base: Base = CONTRATOS,
) -> pd.DataFrame:
    """Fusiona ``new_rows`` con la partición existente (si la hay) y reescribe.

    Nunca elimina contratos: los ids que no vienen en ``new_rows`` se conservan.
    Devuelve el contenido escrito de la partición.
    """
    existing = read_partition(store, partition, base)
    return _apply_merge(store, partition, existing, new_rows, detected_at, base)


def write_new_partition(
    store: ObjectStore,
    partition: str,
    rows: pd.DataFrame,
    *,
    detected_at: datetime | None = None,
    base: Base = CONTRATOS,
) -> pd.DataFrame:
    """Escribe una partición desde cero (sólo tests/partición inexistente)."""
    return _apply_merge(store, partition, None, rows, detected_at, base)


def move_out_ids(
    store: ObjectStore,
    partition: str,
    ids: set[str],
    *,
    motivo: str,
    detected_at: datetime | None = None,
    base: Base = CONTRATOS,
) -> pd.DataFrame | None:
    """Saca ``ids`` de ``partition`` guardándolos antes en el historial.

    Se usa cuando una versión más completa del contrato ya quedó escrita en
    otra partición (``year=unknown`` -> año real). Lee la partición real, así
    que no depende de que el índice esté al día. Devuelve el contenido escrito,
    o ``None`` si no había nada que mover.
    """
    existing = read_partition(store, partition, base)
    if existing is None or not ids or base.key not in existing.columns:
        return None
    leaving = existing[base.key].isin(ids)
    if not leaving.any():
        return None
    history.append(
        store,
        existing[leaving],
        partition=partition,
        motivo=motivo,
        detected_at=detected_at,
        base=base,
    )
    remaining = existing[~leaving].reset_index(drop=True)
    write_partition(store, partition, remaining, base)
    logger.info("%s: %d contratos movidos (%s)", partition, int(leaving.sum()), motivo)
    return remaining


def list_partitions(store: ObjectStore, base: Base = CONTRATOS) -> list[str]:
    """Particiones donde ``base`` tiene archivo, en orden."""
    prefix = f"{base.lake_prefix}/"
    parts = {
        key[len(prefix) :].split("/", 1)[0]
        for key in store.list_keys(prefix)
        if key.endswith(f"/{base.data_file}")
    }
    return sorted(parts)
