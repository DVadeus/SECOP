"""Capa de consulta: DuckDB sobre el Parquet del lake -> pandas -> Excel/CSV."""

from __future__ import annotations

from secop.query.engine import QueryEngine
from secop.query.export import export_dataframe

__all__ = ["QueryEngine", "export_dataframe"]
