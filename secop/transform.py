"""Transformación de registros crudos de Socrata a un DataFrame tipado.

Migrado de ``src/transform.py``. La clasificación de columnas vive en
``secop.schema``. Regla del proyecto (memory-bank/datasetContract.md): nunca se
reemplazan NULLs automáticamente.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import pandas as pd

from secop import schema
from secop.bases import CONTRATOS, Base, SchemaSpec

logger = logging.getLogger(__name__)


def _flatten_url_structs(df: pd.DataFrame, spec: SchemaSpec) -> None:
    """Aplana columnas ``{"url": "..."}`` de Socrata al valor de texto."""
    for col in spec.url_struct_columns:
        if col not in df.columns:
            continue
        df[col] = df[col].map(lambda v: v.get("url") if isinstance(v, Mapping) else v)


def _coerce_dates(df: pd.DataFrame, spec: SchemaSpec) -> None:
    min_ts = pd.Timestamp(schema.DATE_MIN)
    max_ts = pd.Timestamp(schema.DATE_MAX)
    for col in spec.date_columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce")
        out_of_range = (df[col] < min_ts) | (df[col] > max_ts)
        df.loc[out_of_range, col] = pd.NaT


def _coerce_numeric(df: pd.DataFrame, spec: SchemaSpec) -> None:
    # float64 siempre (no int64) para que las particiones no deriven de tipo al
    # concatenarse: unas con nulos (float) y otras sin nulos (int).
    for col in spec.numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")


def _coerce_boolean(df: pd.DataFrame, spec: SchemaSpec) -> None:
    mapping = {schema.BOOLEAN_TRUE: True, schema.BOOLEAN_FALSE: False}
    for col in spec.boolean_columns:
        if col in df.columns:
            df[col] = df[col].map(mapping).astype("boolean")


def _coerce_categorical(df: pd.DataFrame, spec: SchemaSpec) -> None:
    for col in spec.categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype("category")


def to_dataframe(records: list[dict[str, Any]], base: Base = CONTRATOS) -> pd.DataFrame:
    """Convierte una lista de registros crudos en un DataFrame tipado según ``base``."""
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df

    df.columns = df.columns.str.lower()
    spec = base.schema
    _flatten_url_structs(df, spec)
    _coerce_dates(df, spec)
    _coerce_numeric(df, spec)
    _coerce_boolean(df, spec)
    _coerce_categorical(df, spec)

    logger.debug("Transformadas %d filas x %d columnas", len(df), df.shape[1])
    return df
