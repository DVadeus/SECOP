"""Pruebas de secop.history (historial de versiones reemplazadas)."""

from __future__ import annotations

import io
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from secop import history
from secop.storage.local import LocalStore

_AT = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)


def _read(store: LocalStore) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(store.get_bytes(history.history_key(_AT))))


def test_history_key_partitions_by_detection_day() -> None:
    assert (
        history.history_key(_AT)
        == "raw/contratos_historial/detectado=2026-10-10/data.parquet"
    )


def test_content_hash_treats_none_and_nan_as_same_null() -> None:
    a = pd.DataFrame({"id_contrato": ["A"], "x": pd.Series([None], dtype=object)})
    b = pd.DataFrame({"id_contrato": ["A"], "x": pd.Series([np.nan], dtype=object)})
    assert history.content_hash(a).iat[0] == history.content_hash(b).iat[0]


def test_content_hash_ignores_column_order_and_detects_changes() -> None:
    a = pd.DataFrame({"id_contrato": ["A"], "v": [1.0]})
    b = pd.DataFrame({"v": [1.0], "id_contrato": ["A"]})
    c = pd.DataFrame({"id_contrato": ["A"], "v": [2.0]})
    assert history.content_hash(a).iat[0] == history.content_hash(b).iat[0]
    assert history.content_hash(a).iat[0] != history.content_hash(c).iat[0]


def test_append_records_metadata(store: LocalStore) -> None:
    rows = pd.DataFrame({"id_contrato": ["A"], "valor_del_contrato": [100.0]})
    added = history.append(
        store,
        rows,
        partition="year=2024",
        motivo=history.MOTIVO_VERSION,
        detected_at=_AT,
    )
    assert added == 1
    out = _read(store)
    assert out.loc[0, history.COL_YEAR] == "2024"
    assert out.loc[0, history.COL_MOTIVO] == history.MOTIVO_VERSION
    assert out.loc[0, history.COL_DETECTADO] == pd.Timestamp("2026-10-10 12:00")


def test_append_is_idempotent_on_retry(store: LocalStore) -> None:
    rows = pd.DataFrame({"id_contrato": ["A"], "valor_del_contrato": [100.0]})
    kwargs = {
        "partition": "year=2024",
        "motivo": history.MOTIVO_VERSION,
        "detected_at": _AT,
    }
    history.append(store, rows, **kwargs)
    assert history.append(store, rows, **kwargs) == 0
    assert len(_read(store)) == 1


def test_append_keeps_every_distinct_version(store: LocalStore) -> None:
    for valor in (100.0, 200.0):
        history.append(
            store,
            pd.DataFrame({"id_contrato": ["A"], "valor_del_contrato": [valor]}),
            partition="year=2024",
            motivo=history.MOTIVO_VERSION,
            detected_at=_AT,
        )
    assert sorted(_read(store)["valor_del_contrato"]) == [100.0, 200.0]


def test_append_empty_writes_nothing(store: LocalStore) -> None:
    added = history.append(
        store,
        pd.DataFrame({"id_contrato": []}),
        partition="year=2024",
        motivo=history.MOTIVO_VERSION,
    )
    assert added == 0
    assert store.list_keys("raw/contratos_historial") == []
