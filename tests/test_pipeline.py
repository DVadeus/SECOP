"""Pruebas de secop.pipeline con el origen simulado (sin red)."""

from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd
import pytest

from secop import index, lake, pipeline
from secop.config import MANIFEST_KEY, RUN_MARKER_KEY, STATE_KEY
from secop.storage.local import LocalStore
from secop.transform import to_dataframe

Record = dict[str, Any]


def _rec(
    id_contrato: str,
    inicio: str | None = None,
    ua: str | None = None,
    valor: str = "1",
) -> Record:
    """Registro crudo como lo entrega Socrata (las claves nulas no vienen)."""
    raw = {
        "id_contrato": id_contrato,
        "fecha_de_inicio_del_contrato": inicio,
        "ultima_actualizacion": ua,
        "valor_del_contrato": valor,
    }
    return {k: v for k, v in raw.items() if v is not None}


def _seed(store: LocalStore, partition: str, records: list[Record]) -> None:
    lake.write_new_partition(store, partition, to_dataframe(records))


def _ids(store: LocalStore) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for partition in lake.list_partitions(store):
        df = lake.read_partition(store, partition)
        assert df is not None
        out[partition] = sorted(df["id_contrato"])
    return out


def _history(store: LocalStore) -> pd.DataFrame:
    keys = store.list_keys("raw/contratos_historial")
    return pd.concat(pd.read_parquet(io.BytesIO(store.get_bytes(k))) for k in keys)


def _fake_source(monkeypatch: pytest.MonkeyPatch, records: list[Record]) -> None:
    """El origen publica ``records``: foto ligera + descarga por id."""
    snapshot = pd.DataFrame.from_records(records)

    def by_ids(ids: list[str], **_kwargs: object) -> list[Record]:
        wanted = set(ids)
        return [r for r in records if r["id_contrato"] in wanted]

    monkeypatch.setattr(pipeline, "fetch_snapshot", lambda _cols, **_kw: snapshot)
    monkeypatch.setattr(pipeline, "fetch_by_ids", by_ids)


# --------------------------------------------------------------------------- #
# Reglas entre particiones (incremental)                                      #
# --------------------------------------------------------------------------- #
def test_incremental_applies_partition_rules(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(store, lake.UNKNOWN_PARTITION, [_rec("A"), _rec("Z")])
    _seed(
        store,
        "year=2024",
        [
            _rec("B", "2024-01-01T00:00:00", "2024-01-01T00:00:00"),
            _rec("C", "2024-02-01T00:00:00", "2024-01-01T00:00:00"),
        ],
    )
    _fake_source(
        monkeypatch,
        [
            _rec("A", "2025-03-01T00:00:00", "2026-10-01T00:00:00", "5"),  # fecha
            _rec("B", "2025-05-01T00:00:00", "2026-10-01T00:00:00", "7"),  # año
            _rec("C", None, "2026-10-01T00:00:00", "9"),  # pierde fecha
            _rec("D", None, None),  # nuevo sin fecha ni versión
            # Z ya no está en el origen: debe seguir en el lake.
        ],
    )

    pipeline.incremental(store)

    assert _ids(store) == {
        "year=2024": ["B", "C"],  # B se conserva como evidencia; C con fecha
        "year=2025": ["A", "B"],
        lake.UNKNOWN_PARTITION: ["D", "Z"],
    }
    hist = _history(store).set_index("id_contrato")
    assert hist.loc["A", "historial_motivo"] == "fecha_completada"
    assert hist.loc["C", "historial_motivo"] == "fecha_eliminada_en_origen"
    assert hist.loc["C", "valor_del_contrato"] == 9.0


def test_incremental_fetches_new_contracts_without_watermark(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(store, "year=2026", [_rec("A", "2026-01-01T00:00:00", "2026-01-01T00:00:00")])
    _fake_source(
        monkeypatch,
        [
            _rec("A", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
            _rec("N", "2026-02-01T00:00:00", None),  # nuevo, ua nula
        ],
    )
    result = pipeline.incremental(store)
    assert _ids(store)["year=2026"] == ["A", "N"]
    assert result.partitions == {"year=2026": 2}


def test_incremental_updates_version_and_keeps_old_in_history(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(
        store,
        "year=2026",
        [_rec("A", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "100")],
    )
    _fake_source(
        monkeypatch, [_rec("A", "2026-01-01T00:00:00", "2026-09-01T00:00:00", "999")]
    )
    pipeline.incremental(store)
    current = lake.read_partition(store, "year=2026")
    assert current is not None
    assert current["valor_del_contrato"].tolist() == [999.0]
    assert _history(store)["valor_del_contrato"].tolist() == [100.0]


def test_incremental_without_changes_writes_no_partition(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    rec = _rec("A", "2026-01-01T00:00:00", "2026-01-01T00:00:00")
    _seed(store, "year=2026", [rec])
    _fake_source(monkeypatch, [rec])
    result = pipeline.incremental(store)
    assert result.partitions == {}
    assert result.contracts_written == 0


def test_run_result_counts_contracts_not_partition_sizes(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Varios lotes sobre una misma partición no deben sumar su tamaño cada vez."""
    _seed(
        store,
        "year=2026",
        [
            _rec(f"OLD{n}", "2026-01-01T00:00:00", "2026-01-01T00:00:00")
            for n in range(5)
        ],
    )
    _fake_source(
        monkeypatch,
        [
            _rec(f"NEW{n}", "2026-02-01T00:00:00", "2026-09-01T00:00:00")
            for n in range(3)
        ],
    )
    monkeypatch.setattr(pipeline, "FETCH_CHUNK_IDS", 1)  # 3 lotes

    result = pipeline.incremental(store)

    assert result.contracts_written == 3
    assert result.partitions == {"year=2026": 8}  # tamaño final, no 6 + 7 + 8


def test_state_watermark_is_informative_maximum(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_source(
        monkeypatch,
        [
            _rec("A", "2026-01-01T00:00:00", "2026-03-01T00:00:00"),
            _rec("B", "2025-01-01T00:00:00", "2026-07-15T00:00:00"),
        ],
    )
    result = pipeline.incremental(store)
    assert result.new_watermark == "2026-07-15T00:00:00+00:00"
    state = json.loads(store.get_bytes(STATE_KEY))
    assert state["ultima_actualizacion"] == "2026-07-15T00:00:00+00:00"
    assert not store.exists(RUN_MARKER_KEY)


# --------------------------------------------------------------------------- #
# Backfill                                                                    #
# --------------------------------------------------------------------------- #
def test_resumed_backfill_counts_skipped_years(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(
        store,
        "year=2018",
        [_rec("X1", "2018-01-01T00:00:00"), _rec("X2", "2018-02-01T00:00:00")],
    )
    monkeypatch.setattr(
        pipeline,
        "_fetch_year",
        lambda y, _base: to_dataframe([_rec("Y1", f"{y}-03-01T00:00:00")]),
    )
    monkeypatch.setattr(pipeline, "_fetch_tail", lambda a, b, _base: pd.DataFrame())

    result = pipeline.backfill(store, start_year=2018, end_year=2019)

    assert result.skipped == ["year=2018"]
    manifest = json.loads(store.get_bytes(MANIFEST_KEY))
    assert manifest["row_counts"] == {"year=2018": 2, "year=2019": 1}
    assert manifest["total_rows"] == 3


def test_forced_backfill_never_deletes_contracts(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(
        store,
        "year=2024",
        [
            _rec("BORRADO_EN_ORIGEN", "2024-01-01T00:00:00"),
            _rec("B", "2024-02-01T00:00:00"),
        ],
    )
    monkeypatch.setattr(
        pipeline,
        "_fetch_year",
        lambda y, _base: to_dataframe([_rec("B", f"{y}-02-01T00:00:00", valor="2")]),
    )
    pipeline.backfill(store, start_year=2024, end_year=2024, only_year=2024, force=True)
    assert _ids(store)["year=2024"] == ["B", "BORRADO_EN_ORIGEN"]


# --------------------------------------------------------------------------- #
# Robustez                                                                    #
# --------------------------------------------------------------------------- #
def test_crashed_run_rebuilds_index_and_avoids_duplicates(
    store: LocalStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    index.save(store, index.empty())
    _fake_source(monkeypatch, [_rec("X")])

    def crash(*_args: object) -> None:
        raise RuntimeError("caída")

    monkeypatch.setattr(pipeline, "_finish", crash)
    with pytest.raises(RuntimeError):
        pipeline.incremental(store)  # escribe X en unknown, no guarda índice
    assert store.exists(RUN_MARKER_KEY)

    monkeypatch.undo()
    _fake_source(monkeypatch, [_rec("X", "2025-01-01T00:00:00")])
    pipeline.incremental(store)

    assert _ids(store) == {"year=2025": ["X"], lake.UNKNOWN_PARTITION: []}
    assert not store.exists(RUN_MARKER_KEY)


def test_repair_reports_then_moves_unknown_copies(store: LocalStore) -> None:
    _seed(store, lake.UNKNOWN_PARTITION, [_rec("A"), _rec("Z")])
    _seed(
        store,
        "year=2026",
        [
            _rec("A", "2026-01-01T00:00:00", "2026-10-01T00:00:00"),
            _rec("B", "2026-02-01T00:00:00"),
        ],
    )
    _seed(store, "year=2025", [_rec("B", "2025-02-01T00:00:00")])

    dry = pipeline.repair(store)
    assert (dry.unknown_with_date, dry.year_changes) == (1, 1)
    assert not store.list_keys("backup")
    assert _ids(store)[lake.UNKNOWN_PARTITION] == ["A", "Z"]

    done = pipeline.repair(store, apply=True)
    assert done.backup_prefix is not None
    assert store.exists(f"{done.backup_prefix}/raw/contratos/year=unknown/data.parquet")
    assert _ids(store) == {
        "year=2025": ["B"],
        "year=2026": ["A", "B"],  # el cambio de año no se toca
        lake.UNKNOWN_PARTITION: ["Z"],
    }
    assert _history(store)["id_contrato"].tolist() == ["A"]
    assert pipeline.repair(store).unknown_with_date == 0


def _year_change_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage() for r in caplog.records if "cambiaron de año" in r.getMessage()
    ]


def test_year_change_warning_ignores_known_two_year_contracts(
    store: LocalStore,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Caso real: CO1.PCCNTR.7395172 ya estaba en 2025 y 2026; sólo cambió en 2026."""
    _seed(store, "year=2025", [_rec("K", "2025-06-01T00:00:00")])
    _seed(store, "year=2026", [_rec("K", "2026-01-01T00:00:00", "2026-09-01T00:00:00")])
    _fake_source(monkeypatch, [_rec("K", "2026-01-01T00:00:00", "2026-10-05T00:00:00")])

    with caplog.at_level("WARNING", logger="secop.pipeline"):
        result = pipeline.incremental(store)

    assert result.contracts_written == 1
    assert _year_change_warnings(caplog) == []


def test_year_change_warning_reports_new_changes(
    store: LocalStore,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed(store, "year=2025", [_rec("M", "2025-06-01T00:00:00")])
    _fake_source(monkeypatch, [_rec("M", "2026-01-01T00:00:00", "2026-10-05T00:00:00")])

    with caplog.at_level("WARNING", logger="secop.pipeline"):
        pipeline.incremental(store)

    warnings = _year_change_warnings(caplog)
    assert len(warnings) == 1
    assert warnings[0].startswith("1 contratos") and "M" in warnings[0]
    assert _ids(store) == {"year=2025": ["M"], "year=2026": ["M"]}
