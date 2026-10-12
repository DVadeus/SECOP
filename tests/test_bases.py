"""Pruebas de secop.bases (definición de las bases del lake)."""

from __future__ import annotations

import pytest

from secop import config, history, index, lake, schema
from secop.bases import BASES, CONTRATOS, INTERADMINISTRATIVOS, get_base


def test_contratos_keeps_the_original_layout() -> None:
    """La base original no cambia de rutas ni de claves en R2."""
    assert CONTRATOS.dataset_id == config.DATASET_ID
    assert CONTRATOS.data_file == config.DATA_FILE == "data.parquet"
    assert CONTRATOS.lake_prefix == config.LAKE_PREFIX
    assert CONTRATOS.history_prefix == config.HISTORY_PREFIX
    assert CONTRATOS.state_key == config.STATE_KEY
    assert CONTRATOS.index_key == config.INDEX_KEY
    assert CONTRATOS.manifest_key == config.MANIFEST_KEY
    assert CONTRATOS.run_marker_key == config.RUN_MARKER_KEY
    assert CONTRATOS.key == config.PRIMARY_KEY
    assert CONTRATOS.version_column == config.WATERMARK_COLUMN
    assert CONTRATOS.partition_date_column == config.PARTITION_DATE_COLUMN
    assert CONTRATOS.schema.date_columns == schema.DATE_COLUMNS
    assert CONTRATOS.resource_url == config.SOCRATA_RESOURCE_URL
    assert CONTRATOS.metadata_url == config.SOCRATA_METADATA_URL
    assert not CONTRATOS.grouped
    assert CONTRATOS.column_map == {}


def test_interadministrativos_shares_year_folders_with_its_own_files() -> None:
    i = INTERADMINISTRATIVOS
    assert i.dataset_id == "s484-c9k3"
    assert i.lake_prefix == CONTRATOS.lake_prefix
    assert i.history_prefix == CONTRATOS.history_prefix
    assert i.data_file == "interadministrativos.parquet"
    assert lake.data_key("year=2025", i) == (
        "raw/contratos/year=2025/interadministrativos.parquet"
    )
    own = {i.data_file, i.state_key, i.index_key, i.manifest_key, i.run_marker_key}
    theirs = {
        CONTRATOS.data_file,
        CONTRATOS.state_key,
        CONTRATOS.index_key,
        CONTRATOS.manifest_key,
        CONTRATOS.run_marker_key,
    }
    assert not own & theirs


def test_interadministrativos_column_map() -> None:
    mapping = INTERADMINISTRATIVOS.column_map
    assert len(mapping) == 22
    assert len(set(mapping.values())) == 22  # ningún destino repetido
    assert mapping["identificacion_contratista"] == "documento_proveedor"
    assert mapping["id_entidad"] == "codigo_entidad"
    assert mapping["tipo_contratista"] == "tipodocproveedor"
    assert "unspsc_id_clase" not in mapping  # nombre de clase, no código


def test_get_base() -> None:
    assert set(BASES) == {"contratos", "interadministrativos"}
    assert get_base("interadministrativos") is INTERADMINISTRATIVOS
    with pytest.raises(ValueError, match="base desconocida"):
        get_base("otra")


def test_history_key_and_index_key_per_base() -> None:
    from datetime import UTC, datetime

    at = datetime(2026, 10, 12, tzinfo=UTC)
    assert history.history_key(at, INTERADMINISTRATIVOS).endswith(
        "detectado=2026-10-12/interadministrativos.parquet"
    )
    assert history.history_key(at).endswith("detectado=2026-10-12/data.parquet")
    assert index.COLUMNS == ["id_contrato", "partition", "ultima_actualizacion"]
