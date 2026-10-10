"""Pruebas de secop.index (índice de ids del lake)."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from secop import index, lake
from secop.storage.local import LocalStore


def _rows(ids: list[str], ua: str | None = "2026-01-01") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_contrato": ids,
            "ultima_actualizacion": pd.to_datetime([ua] * len(ids)),
            "valor_del_contrato": [1.0] * len(ids),
        }
    )


def _seed(store: LocalStore) -> None:
    lake.write_new_partition(store, "year=2024", _rows(["A", "B"]))
    lake.write_new_partition(store, "year=2025", _rows(["B"], "2026-05-01"))
    lake.write_new_partition(store, lake.UNKNOWN_PARTITION, _rows(["Z"], None))


def test_build_from_lake_has_one_entry_per_id_and_partition(
    store: LocalStore,
) -> None:
    _seed(store)
    idx = index.build_from_lake(store)
    pairs = sorted(zip(idx["id_contrato"], idx["partition"], strict=True))
    assert pairs == [
        ("A", "year=2024"),
        ("B", "year=2024"),
        ("B", "year=2025"),
        ("Z", "year=unknown"),
    ]


def test_row_counts_cover_whole_lake(store: LocalStore) -> None:
    _seed(store)
    counts = index.row_counts(index.build_from_lake(store))
    assert counts == {"year=2024": 2, "year=2025": 1, "year=unknown": 1}


def test_replace_partition_only_touches_that_partition(store: LocalStore) -> None:
    _seed(store)
    idx = index.build_from_lake(store)
    idx = index.replace_partition(idx, "year=2024", _rows(["A", "B", "C"]))
    assert index.row_counts(idx) == {
        "year=2024": 3,
        "year=2025": 1,
        "year=unknown": 1,
    }


def test_max_watermark_is_informative_maximum(store: LocalStore) -> None:
    _seed(store)
    top = index.max_watermark(index.build_from_lake(store))
    assert top == datetime(2026, 5, 1, tzinfo=UTC)
    assert index.max_watermark(index.empty()) is None


def test_save_load_round_trip_and_load_or_build(store: LocalStore) -> None:
    assert index.load(store) is None
    _seed(store)
    built = index.load_or_build(store)  # no existe -> se reconstruye
    index.save(store, built)
    loaded = index.load(store)
    assert loaded is not None
    assert len(loaded) == len(built) == 4
    assert loaded["ultima_actualizacion"].dtype == "datetime64[ns]"
