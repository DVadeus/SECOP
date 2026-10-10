"""Pruebas de secop.lake (partición + compactación/upsert)."""

from __future__ import annotations

import io
from datetime import UTC, datetime

import pandas as pd

from secop import lake
from secop.storage.local import LocalStore


def _frame(rows: list[tuple[str, str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "id_contrato",
            "ultima_actualizacion",
            "fecha_de_inicio_del_contrato",
            "valor_del_contrato",
        ],
    ).astype(
        {
            "ultima_actualizacion": "datetime64[ns]",
            "fecha_de_inicio_del_contrato": "datetime64[ns]",
        }
    )


def test_partition_for_year_routes_unknown() -> None:
    current = datetime.now(UTC).year
    assert lake.partition_for_year(2024) == "year=2024"
    assert lake.partition_for_year(None) == lake.UNKNOWN_PARTITION
    assert lake.partition_for_year(1998) == lake.UNKNOWN_PARTITION
    assert lake.partition_for_year(current + 10) == lake.UNKNOWN_PARTITION


def test_assign_partitions_handles_nat_and_garbage_years() -> None:
    df = _frame(
        [
            ("A", "2026-01-01", "2024-03-01", 1.0),
            ("B", "2026-01-01", "2035-03-01", 2.0),  # año basura -> unknown
            ("C", "2026-01-01", "NaT", 3.0),
        ]
    )
    df.loc[2, "fecha_de_inicio_del_contrato"] = pd.NaT
    parts = lake.assign_partitions(df).tolist()
    assert parts == ["year=2024", lake.UNKNOWN_PARTITION, lake.UNKNOWN_PARTITION]


def test_compact_keeps_latest_version_per_pk() -> None:
    df = _frame(
        [
            ("A", "2026-01-01", "2025-01-01", 100.0),
            ("A", "2026-06-01", "2025-01-01", 999.0),  # versión más nueva
            ("B", "2026-02-01", "2025-02-01", 200.0),
        ]
    )
    out = lake.compact(df).set_index("id_contrato")
    assert len(out) == 2
    assert out.loc["A", "valor_del_contrato"] == 999.0


def test_upsert_partition_merges_and_dedups(store: LocalStore) -> None:
    base = _frame(
        [
            ("A", "2026-01-01", "2025-01-01", 100.0),
            ("B", "2026-01-02", "2025-02-01", 200.0),
        ]
    )
    lake.write_new_partition(store, "year=2025", base)

    update = _frame(
        [
            ("B", "2026-07-01", "2025-02-01", 555.0),  # actualización
            ("C", "2026-07-02", "2025-03-01", 300.0),  # nuevo
        ]
    )
    lake.upsert_partition(store, "year=2025", update)

    out = lake.read_partition(store, "year=2025")
    assert out is not None
    assert sorted(out["id_contrato"]) == ["A", "B", "C"]
    assert out["id_contrato"].duplicated().sum() == 0
    assert out.set_index("id_contrato").loc["B", "valor_del_contrato"] == 555.0


def test_read_missing_partition_returns_none(store: LocalStore) -> None:
    assert lake.read_partition(store, "year=1999") is None


# --------------------------------------------------------------------------- #
# merge_versions + historial                                                  #
# --------------------------------------------------------------------------- #
def _history(store: LocalStore) -> pd.DataFrame:
    keys = store.list_keys("raw/contratos_historial")
    if not keys:
        return pd.DataFrame()
    return pd.concat(pd.read_parquet(io.BytesIO(store.get_bytes(k))) for k in keys)


def test_merge_versions_newer_wins_and_older_is_superseded() -> None:
    old = _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    new = _frame([("A", "2026-06-01", "2025-01-01", 999.0)])
    result = lake.merge_versions(old, new)
    assert result.rows["valor_del_contrato"].tolist() == [999.0]
    assert result.superseded["valor_del_contrato"].tolist() == [100.0]


def test_merge_versions_keeps_existing_when_incoming_is_older() -> None:
    existing = _frame([("A", "2026-06-01", "2025-01-01", 999.0)])
    incoming = _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    result = lake.merge_versions(existing, incoming)
    assert result.rows["valor_del_contrato"].tolist() == [999.0]
    # La versión vieja que llegó tampoco se pierde.
    assert result.superseded["valor_del_contrato"].tolist() == [100.0]


def test_merge_versions_tie_prefers_incoming() -> None:
    existing = _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    incoming = _frame([("A", "2026-01-01", "2025-01-01", 555.0)])
    result = lake.merge_versions(existing, incoming)
    assert result.rows["valor_del_contrato"].tolist() == [555.0]


def test_merge_versions_identical_versions_are_not_history() -> None:
    row = _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    result = lake.merge_versions(row, row.copy())
    assert len(result.rows) == 1
    assert result.superseded.empty


def test_merge_versions_null_watermark_loses_to_dated_version() -> None:
    existing = _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    incoming = _frame([("A", "NaT", "2025-01-01", 200.0)])
    incoming["ultima_actualizacion"] = pd.NaT
    result = lake.merge_versions(existing, incoming)
    assert result.rows["valor_del_contrato"].tolist() == [100.0]


def test_upsert_records_replaced_version_in_history(store: LocalStore) -> None:
    lake.write_new_partition(
        store, "year=2025", _frame([("A", "2026-01-01", "2025-01-01", 100.0)])
    )
    lake.upsert_partition(
        store, "year=2025", _frame([("A", "2026-06-01", "2025-01-01", 999.0)])
    )
    hist = _history(store)
    assert hist["valor_del_contrato"].tolist() == [100.0]
    assert hist["historial_motivo"].tolist() == ["version_reemplazada"]
    assert hist["year"].tolist() == ["2025"]


def test_upsert_never_deletes_ids_missing_from_new_rows(store: LocalStore) -> None:
    lake.write_new_partition(
        store,
        "year=2025",
        _frame(
            [
                ("A", "2026-01-01", "2025-01-01", 1.0),
                ("B", "2026-01-01", "2025-01-01", 2.0),
            ]
        ),
    )
    # El origen ya no publica A: debe seguir en el lake.
    lake.upsert_partition(
        store, "year=2025", _frame([("B", "2026-02-01", "2025-01-01", 3.0)])
    )
    out = lake.read_partition(store, "year=2025")
    assert out is not None
    assert sorted(out["id_contrato"]) == ["A", "B"]


def test_move_out_ids_saves_to_history_first(store: LocalStore) -> None:
    lake.write_new_partition(
        store,
        lake.UNKNOWN_PARTITION,
        _frame([("A", "2026-01-01", "NaT", 1.0), ("Z", "2026-01-01", "NaT", 2.0)]),
    )
    remaining = lake.move_out_ids(
        store, lake.UNKNOWN_PARTITION, {"A"}, motivo="fecha_completada"
    )
    assert remaining is not None
    assert remaining["id_contrato"].tolist() == ["Z"]
    hist = _history(store)
    assert hist["id_contrato"].tolist() == ["A"]
    assert hist["historial_motivo"].tolist() == ["fecha_completada"]


def test_move_out_ids_without_matches_does_not_write(store: LocalStore) -> None:
    lake.write_new_partition(
        store, lake.UNKNOWN_PARTITION, _frame([("Z", "2026-01-01", "NaT", 2.0)])
    )
    assert lake.move_out_ids(store, lake.UNKNOWN_PARTITION, {"A"}, motivo="x") is None
    assert lake.move_out_ids(store, "year=1999", {"A"}, motivo="x") is None


def test_canonicalize_keeps_python_booleans() -> None:
    """Una columna booleana que llega como object True/False no debe anularse."""
    df = pd.DataFrame({"es_pyme": pd.Series([True, False, None], dtype=object)})
    out = lake._canonicalize_dtypes(df)
    assert out["es_pyme"].tolist()[:2] == [True, False]
    assert pd.isna(out["es_pyme"].iat[2])
