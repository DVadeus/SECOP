"""Pruebas de la consulta por base: vistas separadas, filtros mapeados y CLI."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from secop import lake
from secop.bases import CONTRATOS, INTERADMINISTRATIVOS
from secop.cli import app
from secop.query import QueryEngine
from secop.query.filters import (
    QuerySpec,
    UnsupportedFilterError,
    build_history_query,
    build_query,
)
from secop.storage.local import LocalStore

INTER = INTERADMINISTRATIVOS


@pytest.fixture
def two_bases(tmp_path: Path) -> Path:
    """Lake local con ambas bases en las mismas carpetas year=*."""
    root = tmp_path / "data" / "lake"
    store = LocalStore(root)
    contratos = pd.DataFrame(
        {
            "id_contrato": ["CO1.PCCNTR.1", "CO1.PCCNTR.2"],
            "nit_entidad": ["800", "900"],
            "codigo_entidad": ["700100", "700200"],
            "documento_proveedor": ["11", "22"],
            "fecha_de_inicio_del_contrato": pd.to_datetime(
                ["2025-01-01", "2025-02-01"]
            ),
            "ultima_actualizacion": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "valor_del_contrato": [100.0, 200.0],
        }
    )
    lake.write_new_partition(store, "year=2025", contratos)
    inter = pd.DataFrame(
        {
            "id_contrato": ["CO1.PCCNTR.1", "25-22-1", "25-22-1"],
            "id_entidad": ["700100", "273504011", "273504011"],
            "identificacion_contratista": ["11", "33", "44"],
            "contratista": ["UNO", "TRES", "CUATRO"],
            "fuente": ["SECOP_II", "SECOP_I", "SECOP_I"],
            "modalidad_contratacion": ["Contratación directa"] * 3,
            "fecha_inicio_del_contrato": pd.to_datetime(["2025-01-01"] * 3),
            "fecha_cargue": pd.to_datetime(["2025-01-02", "2025-03-01", "2025-03-01"]),
            "valor_con_adiciones": [100.0, 5e9, 5e9],
        }
    )
    lake.write_new_partition(store, "year=2025", inter, base=INTER)
    return root


# --------------------------------------------------------------------------- #
# Motor                                                                       #
# --------------------------------------------------------------------------- #
def test_views_do_not_mix_files_in_the_same_year_folder(two_bases: Path) -> None:
    folder = two_bases / "raw" / "contratos" / "year=2025"
    assert {p.name for p in folder.iterdir()} == {
        "data.parquet",
        "interadministrativos.parquet",
    }
    with QueryEngine.from_source("local", path=two_bases) as qe:
        assert qe.has_view("contratos") and qe.has_view("interadministrativos")
        assert qe.count(CONTRATOS) == 2
        assert qe.count(INTER) == 3
        assert "contratista" not in qe.run("SELECT * FROM contratos LIMIT 1").columns
        assert qe.partition_summary(INTER)["year"].astype(str).tolist() == ["2025"]


def test_missing_base_has_no_view(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "lake")
    lake.write_new_partition(
        store,
        "year=2025",
        pd.DataFrame(
            {
                "id_contrato": ["A"],
                "fecha_de_inicio_del_contrato": pd.to_datetime(["2025-01-01"]),
            }
        ),
    )
    with QueryEngine.from_source("local", path=tmp_path / "lake") as qe:
        assert not qe.has_view("interadministrativos")
        with pytest.raises(LookupError):
            qe.count(INTER)


def test_sql_can_join_both_bases(two_bases: Path) -> None:
    with QueryEngine.from_source("local", path=two_bases) as qe:
        df = qe.run(
            "SELECT c.id_contrato, i.fuente FROM contratos c "
            "JOIN interadministrativos i USING (id_contrato)"
        )
    assert df.values.tolist() == [["CO1.PCCNTR.1", "SECOP_II"]]


# --------------------------------------------------------------------------- #
# Filtros                                                                     #
# --------------------------------------------------------------------------- #
def test_filters_are_translated_with_the_column_map() -> None:
    sql, params = build_query(
        QuerySpec(
            nit_proveedor=["33"],
            proveedor_contains="tres",
            modalidad=["Contratación directa"],
            valor_min=1.0,
            limit=5,
        ),
        INTER,
    )
    assert "FROM interadministrativos" in sql
    assert "identificacion_contratista IN (?)" in sql
    assert "contratista ILIKE ?" in sql
    assert "lower(modalidad_contratacion) IN (?)" in sql
    assert "TRY_CAST(valor_con_adiciones AS DOUBLE) >= ?" in sql
    assert "ORDER BY fecha_cargue DESC" in sql
    assert params == ["33", "%tres%", "contratación directa", 1.0, 5]


def test_contratos_queries_are_unchanged() -> None:
    sql, _ = build_query(QuerySpec(nit_proveedor=["1"], limit=1))
    assert "FROM contratos" in sql and "documento_proveedor IN (?)" in sql
    assert "ORDER BY ultima_actualizacion DESC" in sql


def test_platform_filter_only_exists_in_interadministrativos() -> None:
    sql, params = build_query(QuerySpec(plataforma=["SECOP_I"]), INTER)
    assert "lower(fuente) IN (?)" in sql and params == ["secop_i"]
    with pytest.raises(UnsupportedFilterError, match="--plataforma"):
        build_query(QuerySpec(plataforma=["SECOP_I"]), CONTRATOS)


def test_nit_entidad_in_interadministrativos_uses_contratos(two_bases: Path) -> None:
    sql, params = build_query(QuerySpec(nit_entidad=["800"]), INTER)
    assert "SELECT DISTINCT CAST(codigo_entidad AS VARCHAR) FROM contratos" in sql
    with QueryEngine.from_source("local", path=two_bases) as qe:
        df = qe.run(sql, params)
    assert df["id_contrato"].tolist() == ["CO1.PCCNTR.1"]  # SECOP I no tiene NIT


def test_history_query_per_base() -> None:
    sql, params = build_history_query(["33"], [], INTER)
    assert "FROM interadministrativos_historial" in sql
    assert "identificacion_contratista IN (?)" in sql and params == ["33"]


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #
runner = CliRunner()


def test_cli_query_defaults_to_contratos(two_bases: Path, tmp_path: Path) -> None:
    out = tmp_path / "c.csv"
    args = ["query", "--source", "local", "--path", str(two_bases), "-o", str(out)]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    assert len(pd.read_csv(out)) == 2


def test_cli_query_interadministrativos(two_bases: Path, tmp_path: Path) -> None:
    out = tmp_path / "i.csv"
    result = runner.invoke(
        app,
        [
            "query",
            "--base",
            "interadministrativos",
            "--plataforma",
            "SECOP_I",
            "--source",
            "local",
            "--path",
            str(two_bases),
            "-o",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert sorted(pd.read_csv(out)["contratista"]) == ["CUATRO", "TRES"]


def test_cli_rejects_unknown_base_and_unsupported_filter(two_bases: Path) -> None:
    common = ["--source", "local", "--path", str(two_bases), "-o", "x.csv"]
    bad_base = runner.invoke(app, ["query", "--base", "otra", *common])
    assert bad_base.exit_code == 2
    bad_filter = runner.invoke(app, ["query", "--plataforma", "SECOP_I", *common])
    assert bad_filter.exit_code == 2


def test_cli_info_per_base(two_bases: Path) -> None:
    result = runner.invoke(
        app,
        [
            "info",
            "--base",
            "interadministrativos",
            "--source",
            "local",
            "--path",
            str(two_bases),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "fecha_inicio_del_contrato" in result.output
