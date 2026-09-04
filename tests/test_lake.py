"""Pruebas de secop.lake (partición + compactación/upsert)."""

from __future__ import annotations

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
