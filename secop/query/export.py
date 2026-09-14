"""Exportación de resultados a Excel / CSV / Parquet para la UAN."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Límite de filas de una hoja de Excel (1.048.576) menos la fila de cabecera.
EXCEL_MAX_ROWS = 1_048_575

# openpyxl rechaza los caracteres de control C0 salvo \t, \n y \r
# (ILLEGAL_CHARACTERS_RE en openpyxl.cell.cell). El origen (Socrata) los cuela en
# campos de texto libre, así que se eliminan al escribir el .xlsx. Sólo afecta a
# Excel: CSV y Parquet conservan el texto crudo para poder auditar el dato.
_ILLEGAL_EXCEL_CHARS: dict[int, None] = dict.fromkeys(
    (*range(0x00, 0x09), 0x0B, 0x0C, *range(0x0E, 0x20))
)


def _clean_cell(value: object) -> object:
    """Quita los caracteres de control que openpyxl no admite en una celda."""
    if isinstance(value, str):
        return value.translate(_ILLEGAL_EXCEL_CHARS)
    return value


def _is_textual(series: pd.Series[Any]) -> bool:
    dtype = series.dtype
    if isinstance(dtype, pd.CategoricalDtype | pd.StringDtype):
        return True
    return pd.api.types.is_object_dtype(series)


def _prepare_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Deja el DataFrame en condiciones de pasar por openpyxl.

    Dos ajustes en una sola copia (evita duplicar memoria dos veces en
    exportaciones grandes): los datetimes con tz se pasan a naive y el texto se
    limpia de caracteres de control ilegales.
    """
    out = df.copy()
    cleaned_cells = 0
    dirty_columns: list[str] = []

    for col in out.columns:
        dtype = out[col].dtype
        if isinstance(dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_localize(None)
            continue
        if not _is_textual(out[col]):
            continue
        # Las categóricas se pasan a object: dos categorías distintas podrían
        # limpiarse al mismo valor y rename_categories reventaría.
        original = out[col]
        if isinstance(dtype, pd.CategoricalDtype):
            original = original.astype("object")
        cleaned = original.map(_clean_cell)
        changed = int((cleaned.ne(original) & cleaned.notna()).sum())
        if changed:
            cleaned_cells += changed
            dirty_columns.append(str(col))
        out[col] = cleaned

    if cleaned_cells:
        logger.warning(
            "Se limpiaron caracteres de control en %d celdas para escribir Excel "
            "(columnas: %s). El dato original se conserva en el lake.",
            cleaned_cells,
            ", ".join(dirty_columns),
        )
    return out


def _write_excel(df: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        _prepare_for_excel(df).to_excel(writer, index=False, sheet_name="contratos")
        writer.sheets["contratos"].freeze_panes = "A2"


def export_dataframe(df: pd.DataFrame, path: str | Path) -> Path:
    """Escribe ``df`` a ``path``. El formato lo decide la extensión.

    Si es ``.xlsx`` y hay más filas de las que admite Excel, cae a CSV con el
    mismo nombre base y avisa. Devuelve la ruta realmente escrita.
    """
    path = Path(path)
    suffix = path.suffix.lower()
    path.parent.mkdir(parents=True, exist_ok=True)

    if suffix in {".xlsx", ".xls"}:
        if len(df) > EXCEL_MAX_ROWS:
            csv_path = path.with_suffix(".csv")
            logger.warning(
                "El resultado tiene %d filas y supera el límite de Excel; "
                "se escribe CSV en %s",
                len(df),
                csv_path.name,
            )
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            return csv_path
        _write_excel(df, path)
    elif suffix == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    elif suffix == ".parquet":
        df.to_parquet(path, index=False, compression="snappy")
    else:
        raise ValueError(
            f"Extensión no soportada: {suffix!r}. Usa .xlsx, .csv o .parquet"
        )

    logger.info("Escrito %s (%d filas x %d columnas)", path, len(df), df.shape[1])
    return path
