"""Exportación de resultados a Excel / CSV / Parquet para la UAN."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Límite de filas de una hoja de Excel (1.048.576) menos la fila de cabecera.
EXCEL_MAX_ROWS = 1_048_575


def _strip_timezones(df: pd.DataFrame) -> pd.DataFrame:
    """openpyxl no admite datetimes con tz; se pasan a naive."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_localize(None)
    return out


def _write_excel(df: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        _strip_timezones(df).to_excel(writer, index=False, sheet_name="contratos")
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
