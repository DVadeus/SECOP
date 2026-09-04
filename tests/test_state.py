"""Pruebas de secop.state."""

from __future__ import annotations

from datetime import UTC

from secop.config import DEFAULT_START_YEAR
from secop.state import PipelineState, load_state, save_state
from secop.storage.local import LocalStore


def test_default_state_when_absent(store: LocalStore) -> None:
    state = load_state(store)
    assert state.watermark.year == DEFAULT_START_YEAR
    assert state.watermark.tzinfo is not None


def test_round_trip(store: LocalStore) -> None:
    state = PipelineState(
        ultima_actualizacion="2026-08-01T00:00:00+00:00",
        row_counts={"year=2026": 42},
        schema_version="test",
    )
    save_state(store, state)

    loaded = load_state(store)
    assert loaded.ultima_actualizacion == "2026-08-01T00:00:00+00:00"
    assert loaded.row_counts == {"year=2026": 42}
    assert loaded.schema_version == "test"
    assert loaded.last_run_utc is not None  # lo escribe save_state
    assert loaded.watermark.tzinfo == UTC


def test_load_ignores_unknown_keys(store: LocalStore) -> None:
    store.put_bytes(
        "state/contratos.json",
        b'{"ultima_actualizacion": "2026-01-01T00:00:00+00:00", "campo_raro": 1}',
    )
    state = load_state(store)
    assert state.ultima_actualizacion == "2026-01-01T00:00:00+00:00"
