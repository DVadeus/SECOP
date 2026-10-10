"""Pruebas de secop.diff (qué ids descargar comparando origen e índice)."""

from __future__ import annotations

import pandas as pd

from secop import diff, index

_UNKNOWN = "year=unknown"


def _snapshot(rows: list[tuple[str, str | None, str | None]]) -> pd.DataFrame:
    """Filas (id, ultima_actualizacion, fecha_de_inicio) como llegan de Socrata."""
    return pd.DataFrame(
        rows,
        columns=[
            "id_contrato",
            "ultima_actualizacion",
            "fecha_de_inicio_del_contrato",
        ],
    )


def _idx(rows: list[tuple[str, str, str | None]]) -> pd.DataFrame:
    """Entradas (id, partición, ultima_actualizacion) del índice."""
    df = pd.DataFrame(
        rows, columns=["id_contrato", "partition", "ultima_actualizacion"]
    )
    return index._normalize(df)


def test_new_id_is_fetched_even_without_watermark() -> None:
    snap = _snapshot([("N", None, "2026-03-01T00:00:00.000")])
    assert diff.ids_to_fetch(snap, index.empty()) == ["N"]


def test_unchanged_ids_are_not_fetched() -> None:
    snap = _snapshot(
        [
            ("A", "2026-01-01T00:00:00.000", "2025-01-01T00:00:00.000"),
            ("Z", None, None),
        ]
    )
    idx = _idx([("A", "year=2025", "2026-01-01"), ("Z", _UNKNOWN, None)])
    assert diff.ids_to_fetch(snap, idx) == []


def test_unknown_that_now_has_a_date_is_fetched() -> None:
    snap = _snapshot([("A", None, "2026-03-01T00:00:00.000")])
    idx = _idx([("A", _UNKNOWN, None)])
    assert diff.ids_to_fetch(snap, idx) == ["A"]


def test_year_change_is_fetched() -> None:
    snap = _snapshot([("A", None, "2026-03-01T00:00:00.000")])
    idx = _idx([("A", "year=2025", None)])
    assert diff.ids_to_fetch(snap, idx) == ["A"]


def test_newer_version_in_same_partition_is_fetched() -> None:
    snap = _snapshot(
        [
            ("A", "2026-09-01T00:00:00.000", "2025-01-01T00:00:00.000"),
            ("B", "2026-09-01T00:00:00.000", "2025-01-01T00:00:00.000"),
        ]
    )
    idx = _idx([("A", "year=2025", "2026-01-01"), ("B", "year=2025", None)])
    assert diff.ids_to_fetch(snap, idx) == ["A", "B"]


def test_source_dropping_the_date_is_fetched_only_with_newer_version() -> None:
    idx = _idx([("A", "year=2025", "2026-01-01")])
    same = _snapshot([("A", "2026-01-01T00:00:00.000", None)])
    newer = _snapshot([("A", "2026-09-01T00:00:00.000", None)])
    assert diff.ids_to_fetch(same, idx) == []
    assert diff.ids_to_fetch(newer, idx) == ["A"]


def test_missing_snapshot_column_counts_as_null() -> None:
    snap = pd.DataFrame({"id_contrato": ["N"]})
    assert diff.ids_to_fetch(snap, index.empty()) == ["N"]
