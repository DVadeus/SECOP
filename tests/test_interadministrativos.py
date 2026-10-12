"""Pruebas de la base interadministrativos: tipado, merge agrupado y full_refresh."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd
import pytest

from secop import index, lake, pipeline
from secop.bases import CONTRATOS, INTERADMINISTRATIVOS
from secop.storage.local import LocalStore
from secop.transform import to_dataframe

INTER = INTERADMINISTRATIVOS
Record = dict[str, Any]


def _rec(
    id_contrato: str,
    nit: str = "1",
    origen: str = "SGP",
    inicio: str | None = "2025-01-01T00:00:00.000",
    valor: str = "10",
    fuente: str = "SECOP_I",
) -> Record:
    """Registro crudo de s484-c9k3 (las claves nulas no vienen de Socrata)."""
    raw = {
        "id_contrato": id_contrato,
        "identificacion_contratista": nit,
        "origen_de_los_recursos": origen,
        "fecha_inicio_del_contrato": inicio,
        "fecha_cargue": "2025-01-02T00:00:00.000",
        "valor_con_adiciones": valor,
        "id_entidad": "273504011",
        "fuente": fuente,
        "link": {"url": f"https://example.test/{id_contrato}"},
    }
    return {k: v for k, v in raw.items() if v is not None}


def _frame(records: list[Record]) -> pd.DataFrame:
    return to_dataframe(records, INTER)


def _rows(store: LocalStore, partition: str) -> list[tuple[str, str, float]]:
    df = lake.read_partition(store, partition, INTER)
    assert df is not None
    return sorted(
        zip(
            df["id_contrato"],
            df["origen_de_los_recursos"],
            df["valor_con_adiciones"],
            strict=True,
        )
    )


def _history(store: LocalStore) -> pd.DataFrame:
    keys = [
        k
        for k in store.list_keys("raw/contratos_historial")
        if k.endswith(INTER.data_file)
    ]
    if not keys:
        return pd.DataFrame()
    return pd.concat(pd.read_parquet(io.BytesIO(store.get_bytes(k))) for k in keys)


# --------------------------------------------------------------------------- #
# Tipado                                                                      #
# --------------------------------------------------------------------------- #
def test_transform_with_interadministrativos_schema() -> None:
    df = _frame([_rec("A")])
    assert df.loc[0, "link"] == "https://example.test/A"
    assert df.loc[0, "id_entidad"] == "273504011"  # identificador: queda texto
    assert df.loc[0, "valor_con_adiciones"] == 10.0
    assert pd.api.types.is_datetime64_any_dtype(df["fecha_cargue"])
    assert isinstance(df["fuente"].dtype, pd.CategoricalDtype)


# --------------------------------------------------------------------------- #
# Partición por contrato                                                      #
# --------------------------------------------------------------------------- #
def test_grouped_contract_goes_to_earliest_valid_year() -> None:
    df = _frame(
        [
            _rec("A", inicio=None),
            _rec("A", origen="Propios", inicio="2026-03-01T00:00:00.000"),
            _rec("A", origen="Otro", inicio="2025-06-01T00:00:00.000"),
            _rec("B", inicio="1901-01-01T00:00:00.000"),  # basura -> no cuenta
            _rec("B", origen="Propios", inicio="2024-02-01T00:00:00.000"),
            _rec("C", inicio=None),
        ]
    )
    parts = dict(
        zip(df["id_contrato"], lake.assign_partitions(df, INTER), strict=False)
    )
    assert set(lake.assign_partitions(df[df.id_contrato == "A"], INTER)) == {
        "year=2025"
    }
    assert parts["B"] == "year=2024"
    assert parts["C"] == lake.UNKNOWN_PARTITION


def test_contratos_partitioning_is_still_per_row() -> None:
    df = pd.DataFrame(
        {
            "id_contrato": ["A", "A"],
            "fecha_de_inicio_del_contrato": pd.to_datetime(
                ["2024-01-01", "2025-01-01"]
            ),
        }
    )
    assert lake.assign_partitions(df, CONTRATOS).tolist() == ["year=2024", "year=2025"]


# --------------------------------------------------------------------------- #
# Merge agrupado                                                              #
# --------------------------------------------------------------------------- #
def test_merge_groups_keeps_every_party_and_collapses_identical_rows() -> None:
    result = lake.merge_versions(
        None,
        _frame([_rec("A"), _rec("A", nit="2"), _rec("A", nit="2")]),
        INTER,
    )
    assert len(result.rows) == 2  # dos partes; la fila idéntica se colapsa
    assert result.applied == 1 and result.changed


def test_merge_groups_unchanged_contract_is_not_rewritten() -> None:
    existing = lake.merge_versions(None, _frame([_rec("A"), _rec("A", nit="2")]), INTER)
    again = lake.merge_versions(
        existing.rows, _frame([_rec("A", nit="2"), _rec("A")]), INTER
    )
    assert not again.changed
    assert again.applied == 0
    assert again.superseded.empty


def test_merge_groups_replaces_whole_contract_when_one_row_changes() -> None:
    existing = lake.merge_versions(
        None, _frame([_rec("A"), _rec("A", origen="Propios")]), INTER
    ).rows
    incoming = _frame([_rec("A", valor="99"), _rec("A", origen="Propios")])
    result = lake.merge_versions(existing, incoming, INTER)
    assert sorted(result.rows["valor_con_adiciones"]) == [10.0, 99.0]
    # Toda la versión anterior del contrato (las dos filas) va al historial.
    assert sorted(result.superseded["valor_con_adiciones"]) == [10.0, 10.0]
    assert result.applied == 1


def test_merge_groups_never_deletes_absent_contracts() -> None:
    existing = lake.merge_versions(None, _frame([_rec("A"), _rec("Z")]), INTER).rows
    result = lake.merge_versions(existing, _frame([_rec("A", valor="5")]), INTER)
    assert sorted(result.rows["id_contrato"]) == ["A", "Z"]


def test_upsert_skips_rewrite_when_nothing_changed(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake.upsert(store, "year=2025", _frame([_rec("A")]), base=INTER)
    writes: list[str] = []
    original = store.put_bytes

    def spy(key: str, data: bytes) -> None:
        writes.append(key)
        original(key, data)

    monkeypatch.setattr(store, "put_bytes", spy)
    result = lake.upsert(store, "year=2025", _frame([_rec("A")]), base=INTER)
    assert not result.changed
    assert writes == []


# --------------------------------------------------------------------------- #
# full_refresh (origen simulado)                                              #
# --------------------------------------------------------------------------- #
def _fake_source(monkeypatch: pytest.MonkeyPatch, records: list[Record]) -> None:
    def fetch_all(**kwargs: object) -> list[Record]:
        assert kwargs.get("base") is INTER
        return [dict(r) for r in records]

    monkeypatch.setattr(pipeline, "fetch_all_checked", fetch_all)


def test_full_refresh_loads_then_is_idempotent(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_source(
        monkeypatch,
        [_rec("A"), _rec("A", nit="2"), _rec("B", inicio=None), _rec("A", nit="2")],
    )
    first = pipeline.incremental(store, INTER)
    assert first.contracts_written == 2
    assert set(first.partitions) == {"year=2025", lake.UNKNOWN_PARTITION}

    second = pipeline.incremental(store, INTER)
    assert second.contracts_written == 0
    assert second.partitions == {}
    assert _history(store).empty
    manifest = json.loads(store.get_bytes(INTER.manifest_key))
    assert manifest["base"] == "interadministrativos"
    assert manifest["dataset_id"] == "s484-c9k3"
    assert manifest["total_rows"] == 3


def test_full_refresh_versions_and_never_deletes(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_source(monkeypatch, [_rec("A"), _rec("Z")])
    pipeline.incremental(store, INTER)
    # A cambia de valor y Z desaparece del origen.
    _fake_source(monkeypatch, [_rec("A", valor="99")])
    result = pipeline.incremental(store, INTER)

    assert result.contracts_written == 1
    assert _rows(store, "year=2025") == [("A", "SGP", 99.0), ("Z", "SGP", 10.0)]
    hist = _history(store)
    assert hist["valor_con_adiciones"].tolist() == [10.0]
    assert hist["historial_motivo"].tolist() == ["version_reemplazada"]


def test_full_refresh_moves_contract_out_of_unknown(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_source(monkeypatch, [_rec("A", inicio=None), _rec("A", nit="2", inicio=None)])
    pipeline.incremental(store, INTER)
    _fake_source(
        monkeypatch,
        [_rec("A", inicio=None), _rec("A", nit="2", inicio="2026-01-01T00:00:00.000")],
    )
    pipeline.incremental(store, INTER)

    assert len(_rows(store, "year=2026")) == 2  # el contrato completo
    assert _rows(store, lake.UNKNOWN_PARTITION) == []
    moved = _history(store)
    assert set(moved["historial_motivo"]) == {"fecha_completada"}
    assert len(moved) == 2


def test_full_refresh_never_touches_contratos_files(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    contratos = pd.DataFrame(
        {
            "id_contrato": ["A"],
            "fecha_de_inicio_del_contrato": pd.to_datetime(["2025-01-01"]),
            "ultima_actualizacion": pd.to_datetime(["2026-01-01"]),
        }
    )
    lake.write_new_partition(store, "year=2025", contratos)
    before = store.get_bytes(lake.data_key("year=2025"))

    _fake_source(monkeypatch, [_rec("A", valor="1")])
    pipeline.incremental(store, INTER)

    assert store.get_bytes(lake.data_key("year=2025")) == before
    assert not store.exists(CONTRATOS.state_key)
    assert not store.exists(CONTRATOS.index_key)
    assert lake.list_partitions(store) == ["year=2025"]
    assert lake.list_partitions(store, INTER) == ["year=2025"]
    idx = index.load(store, INTER)
    assert idx is not None and len(idx) == 1


def test_backfill_of_full_refresh_base_ignores_years(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_source(monkeypatch, [_rec("A")])

    def no_years(*_args: object) -> pd.DataFrame:
        raise AssertionError("no debe descargar por años")

    monkeypatch.setattr(pipeline, "_fetch_year", no_years)
    result = pipeline.backfill(store, start_year=2018, end_year=2026, base=INTER)
    assert result.mode == "backfill"
    assert result.contracts_written == 1
