"""Pruebas de la capa de consulta (DuckDB local) y la exportación."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from secop import lake
from secop.query import QueryEngine, export_dataframe
from secop.query.export import EXCEL_MAX_ROWS
from secop.storage.local import LocalStore


@pytest.fixture
def seeded_lake(tmp_path: Path) -> Path:
    root = tmp_path / "data" / "lake"
    store = LocalStore(root)
    df = pd.DataFrame(
        {
            "id_contrato": ["A", "B", "C", "D"],
            "nit_entidad": ["800", "800", "999", "800"],
            "documento_proveedor": ["11", "22", "33", "44"],
            "nombre_entidad": [
                "ALCALDIA DE BOGOTA",
                "ALCALDIA DE BOGOTA",
                "OTRA",
                "ALCALDIA DE BOGOTA",
            ],
            "ultima_actualizacion": pd.to_datetime(
                ["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"]
            ),
            "fecha_de_inicio_del_contrato": pd.to_datetime(
                pd.Series(["2024-01-01", "2025-01-01", "2025-06-01", None])
            ),
            "valor_del_contrato": [100.0, 200.0, 300.0, 400.0],
        }
    )
    lake.write_new_partition(store, "year=2024", df.iloc[[0]])
    lake.write_new_partition(store, "year=2025", df.iloc[[1, 2]])
    # D: fecha_de_inicio_del_contrato nula -> partición year=unknown, misma
    # entidad "800" que A y B. Simula un contrato real sin fecha registrada.
    lake.write_new_partition(store, lake.UNKNOWN_PARTITION, df.iloc[[3]])
    return root


def test_engine_reads_partitions_and_hive_column(seeded_lake: Path) -> None:
    with QueryEngine.from_source("local", path=seeded_lake) as qe:
        assert qe.count() == 4
        summary = qe.partition_summary()
        assert set(summary["year"].astype(str)) == {"2024", "2025", "unknown"}


def test_engine_filter_by_entidad(seeded_lake: Path) -> None:
    from secop.query.filters import QuerySpec, build_query

    sql, params = build_query(QuerySpec(nit_entidad=["800"]))
    with QueryEngine.from_source("local", path=seeded_lake) as qe:
        df = qe.run(sql, params)
    assert sorted(df["id_contrato"]) == ["A", "B", "D"]


def test_year_from_does_not_drop_unknown_year_rows_end_to_end(
    seeded_lake: Path,
) -> None:
    """D (year=unknown, misma entidad) debe seguir apareciendo aunque se pida
    --year-from: no se puede saber si su año está o no dentro del rango."""
    from secop.query.filters import QuerySpec, build_query

    sql, params = build_query(QuerySpec(nit_entidad=["800"], year_from=2025))
    with QueryEngine.from_source("local", path=seeded_lake) as qe:
        df = qe.run(sql, params)
    assert sorted(df["id_contrato"]) == ["B", "D"]


def test_export_excel_and_csv(seeded_lake: Path, tmp_path: Path) -> None:
    with QueryEngine.from_source("local", path=seeded_lake) as qe:
        df = qe.run("SELECT * FROM contratos ORDER BY id_contrato")

    xlsx = export_dataframe(df, tmp_path / "out.xlsx")
    assert xlsx.suffix == ".xlsx" and xlsx.exists()
    assert len(pd.read_excel(xlsx)) == 4

    csv = export_dataframe(df, tmp_path / "out.csv")
    assert csv.exists()


def test_export_excel_falls_back_to_csv_when_too_many_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("secop.query.export.EXCEL_MAX_ROWS", 2)
    df = pd.DataFrame({"x": range(5)})
    written = export_dataframe(df, tmp_path / "big.xlsx")
    assert written.suffix == ".csv"
    assert written.exists()
    _ = EXCEL_MAX_ROWS  # referenciado para el import
