"""Pruebas de la vista combinada contratos_todos y del watermark sin versión."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

from secop import lake, pipeline
from secop.bases import INTERADMINISTRATIVOS
from secop.cli import app
from secop.query import QueryEngine
from secop.query.combined import COMBINED_VIEW, TODAS
from secop.query.filters import QuerySpec, UnsupportedFilterError, build_query
from secop.state import PipelineState, load_state
from secop.storage.local import LocalStore

INTER = INTERADMINISTRATIVOS


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    """Contratos (3) + interadministrativos: 1 solapado, 1 SECOP II nuevo y un
    convenio SECOP I con dos filas (dos partes)."""
    root = tmp_path / "lake"
    store = LocalStore(root)
    contratos = pd.DataFrame(
        {
            "id_contrato": ["CO1.PCCNTR.1", "CO1.PCCNTR.2", "CO1.PCCNTR.3"],
            "nit_entidad": ["800", "800", "900"],
            "codigo_entidad": ["700100", "700100", "700200"],
            "documento_proveedor": ["11", "22", "33"],
            "proveedor_adjudicado": ["UNO S.A.", "DOS", "TRES"],
            "justificacion_modalidad_de": [
                "Contratos o convenios Interadministrativos (con valor)",
                "Contratos o convenios Interadministrativos (valor cero)",
                "Servicios profesionales y de apoyo a la gestión",
            ],
            "codigo_de_categoria_principal": ["V1.1", "V1.2", "V1.3"],
            "tipodocproveedor": ["NIT", "NIT", "NIT"],
            "fecha_de_inicio_del_contrato": pd.to_datetime(
                ["2025-01-01", "2025-02-01", "2025-03-01"]
            ),
            "ultima_actualizacion": pd.to_datetime(
                ["2026-01-01", "2026-01-02", "2026-01-03"]
            ),
            "valor_del_contrato": [100.0, 0.0, 300.0],
        }
    )
    lake.write_new_partition(store, "year=2025", contratos)
    inter = pd.DataFrame(
        {
            "id_contrato": ["CO1.PCCNTR.1", "CO1.PCCNTR.9", "25-22-1", "25-22-1"],
            "id_entidad": ["700100", "700100", "273504011", "273504011"],
            "identificacion_contratista": ["11", "99", "44", "55"],
            # El nombre del solapado difiere: en la vista manda contratos.
            "contratista": ["UNO SA", "NUEVE", "CUATRO", "CINCO"],
            "tipo_contratista": [
                "No Definido",
                "No Definido",
                "Nuip",
                "Nit de Persona Jurídica",
            ],
            "unspsc_id_clase": [
                "V1.1",
                "V1.9",
                "Servicios de salud",
                "Servicios de salud",
            ],
            "fuente": ["SECOP_II", "SECOP_II", "SECOP_I", "SECOP_I"],
            "fecha_inicio_del_contrato": pd.to_datetime(["2025-01-01"] * 4),
            "fecha_cargue": pd.to_datetime(
                ["2025-01-05", "2025-02-05", "2025-03-05", "2025-03-05"]
            ),
            "valor_con_adiciones": [100.0, 900.0, 50.0, 50.0],
        }
    )
    lake.write_new_partition(store, "year=2025", inter, base=INTER)
    return root


def _view(root: Path, sql: str) -> pd.DataFrame:
    with QueryEngine.from_source("local", path=root) as qe:
        return qe.run(sql)


# --------------------------------------------------------------------------- #
# Vista                                                                       #
# --------------------------------------------------------------------------- #
def test_view_requires_both_bases(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "solo")
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
    with QueryEngine.from_source("local", path=tmp_path / "solo") as qe:
        assert not qe.has_view(COMBINED_VIEW)


def test_part_a_keeps_every_contratos_row_once(lake_root: Path) -> None:
    df = _view(
        lake_root, "SELECT * FROM contratos_todos WHERE base_origen = 'contratos'"
    )
    assert sorted(df["id_contrato"]) == ["CO1.PCCNTR.1", "CO1.PCCNTR.2", "CO1.PCCNTR.3"]
    row = df.set_index("id_contrato").loc["CO1.PCCNTR.1"]
    assert row["proveedor_adjudicado"] == "UNO S.A."  # manda contratos
    assert bool(row["en_base_interadministrativos"])
    assert row["fecha_cargue"] == pd.Timestamp("2025-01-05")
    assert row["plataforma"] == "SECOP_II"


def test_interadministrativo_flags(lake_root: Path) -> None:
    df = _view(
        lake_root,
        "SELECT id_contrato, en_base_interadministrativos AS en_base, "
        "es_interadministrativo AS es_inter FROM contratos_todos "
        "WHERE base_origen = 'contratos' ORDER BY 1",
    )
    assert df.values.tolist() == [
        ["CO1.PCCNTR.1", True, True],  # en la base y justificado
        ["CO1.PCCNTR.2", False, True],  # sólo por la justificación
        ["CO1.PCCNTR.3", False, False],
    ]


def test_part_b_renames_columns_and_adds_only_new_ones(lake_root: Path) -> None:
    df = _view(
        lake_root,
        "SELECT * FROM contratos_todos WHERE base_origen = 'interadministrativos' "
        "ORDER BY id_contrato, documento_proveedor",
    )
    assert df["id_contrato"].tolist() == ["25-22-1", "25-22-1", "CO1.PCCNTR.9"]
    assert df["documento_proveedor"].tolist() == ["44", "55", "99"]
    assert df["proveedor_adjudicado"].tolist() == ["CUATRO", "CINCO", "NUEVE"]
    assert df["codigo_entidad"].tolist() == ["273504011", "273504011", "700100"]
    assert df["valor_del_contrato"].tolist() == [50.0, 50.0, 900.0]
    # tipo_contratista se funde con su valor original.
    assert df["tipodocproveedor"].tolist() == [
        "Nuip",
        "Nit de Persona Jurídica",
        "No Definido",
    ]
    # UNSPSC: nombre de clase en SECOP I, código en SECOP II.
    assert df["unspsc_clase_nombre"].tolist()[:2] == ["Servicios de salud"] * 2
    assert pd.isna(df["unspsc_clase_nombre"].iat[2])
    assert pd.isna(df["codigo_de_categoria_principal"].iat[0])
    assert df["codigo_de_categoria_principal"].iat[2] == "V1.9"
    # No se completan datos que el origen no publica.
    assert df["nit_entidad"].isna().all()
    assert df["plataforma"].tolist() == ["SECOP_I", "SECOP_I", "SECOP_II"]
    assert df["year"].astype(str).unique().tolist() == ["2025"]
    for own in ("contratista", "identificacion_contratista", "fuente", "anno_cargue"):
        assert own not in df.columns


def test_view_total_rows(lake_root: Path) -> None:
    with QueryEngine.from_source("local", path=lake_root) as qe:
        assert qe.count(TODAS) == 3 + 3  # contratos + filas de inter fuera de contratos


# --------------------------------------------------------------------------- #
# Filtros                                                                     #
# --------------------------------------------------------------------------- #
def _ids(root: Path, spec: QuerySpec) -> list[str]:
    sql, params = build_query(spec, TODAS)
    with QueryEngine.from_source("local", path=root) as qe:
        return sorted(set(qe.run(sql, params)["id_contrato"]))


def test_nit_entidad_on_combined_view_uses_codigo_for_inter_rows(
    lake_root: Path,
) -> None:
    # 800 -> codigo 700100: CO1.1 y CO1.2 (contratos) y CO1.9 (sólo en inter).
    assert _ids(lake_root, QuerySpec(nit_entidad=["800"])) == [
        "CO1.PCCNTR.1",
        "CO1.PCCNTR.2",
        "CO1.PCCNTR.9",
    ]


def test_other_filters_on_combined_view(lake_root: Path) -> None:
    assert _ids(lake_root, QuerySpec(solo_interadministrativos=True)) == [
        "25-22-1",
        "CO1.PCCNTR.1",
        "CO1.PCCNTR.2",
        "CO1.PCCNTR.9",
    ]
    assert _ids(lake_root, QuerySpec(plataforma=["SECOP_I"])) == ["25-22-1"]
    assert _ids(lake_root, QuerySpec(nit_proveedor=["55"])) == ["25-22-1"]
    assert _ids(lake_root, QuerySpec(valor_min=800)) == ["CO1.PCCNTR.9"]


def test_combined_query_orders_by_any_recency_column() -> None:
    sql, _ = build_query(QuerySpec(limit=1), TODAS)
    assert "FROM contratos_todos" in sql
    assert "ORDER BY coalesce(ultima_actualizacion, fecha_cargue) DESC" in sql


def test_solo_interadministrativos_only_on_combined_view() -> None:
    with pytest.raises(UnsupportedFilterError, match="--base todas"):
        build_query(QuerySpec(solo_interadministrativos=True))
    with pytest.raises(UnsupportedFilterError):
        build_query(QuerySpec(solo_interadministrativos=True), INTER)


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #
runner = CliRunner()


def test_cli_query_todas(lake_root: Path, tmp_path: Path) -> None:
    out = tmp_path / "todas.csv"
    result = runner.invoke(
        app,
        [
            "query",
            "--base",
            "todas",
            "--solo-interadministrativos",
            "--source",
            "local",
            "--path",
            str(lake_root),
            "-o",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    df = pd.read_csv(out)
    assert set(df["base_origen"]) == {"contratos", "interadministrativos"}
    assert df["id_contrato"].nunique() == 4


def test_cli_info_todas_and_historial_rejects_it(lake_root: Path) -> None:
    common = ["--source", "local", "--path", str(lake_root)]
    info = runner.invoke(app, ["info", "--base", "todas", *common])
    assert info.exit_code == 0, info.output
    assert "vista combinada" in info.output
    hist = runner.invoke(
        app,
        [
            "historial",
            "--base",
            "todas",
            "--nit-proveedor",
            "1",
            "-o",
            "x.csv",
            *common,
        ],
    )
    assert hist.exit_code == 2


def test_etl_does_not_accept_todas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Una base inválida falla antes de crear el store (no toca el disco)."""
    target = tmp_path / "no-debe-crearse"
    monkeypatch.setenv("SECOP_LOCAL_DATA_DIR", str(target))
    from secop.config import get_settings

    get_settings.cache_clear()
    try:
        result = runner.invoke(
            app, ["etl", "incremental", "--base", "todas", "--target", "local"]
        )
    finally:
        get_settings.cache_clear()
    assert result.exit_code == 2
    assert not target.exists()


# --------------------------------------------------------------------------- #
# Watermark en bases sin columna de versión                                   #
# --------------------------------------------------------------------------- #
def _inter_rec(id_contrato: str) -> dict[str, Any]:
    return {
        "id_contrato": id_contrato,
        "identificacion_contratista": "1",
        "fecha_inicio_del_contrato": "2025-01-01T00:00:00.000",
        "fecha_cargue": "2025-01-02T00:00:00.000",
        "fuente": "SECOP_I",
    }


def test_full_refresh_base_has_no_watermark(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Estado viejo con el valor por defecto, como el que quedó en R2.
    store.put_bytes(
        INTER.state_key,
        b'{"ultima_actualizacion": "2018-01-01T00:00:00+00:00"}',
    )
    monkeypatch.setattr(pipeline, "fetch_all_checked", lambda **_kw: [_inter_rec("A")])

    result = pipeline.incremental(store, INTER)

    assert result.new_watermark is None
    state = load_state(store, INTER)
    assert state.ultima_actualizacion is None
    assert state.watermark is None
    assert json.loads(store.get_bytes(INTER.manifest_key))["watermark"] is None


def test_contratos_default_watermark_is_unchanged() -> None:
    assert PipelineState().watermark is not None
    assert PipelineState(ultima_actualizacion=None).watermark is None
