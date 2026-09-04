"""Pruebas de secop.transform."""

from __future__ import annotations

import pandas as pd

from secop.transform import to_dataframe


def test_empty_input_returns_empty_frame() -> None:
    assert to_dataframe([]).empty


def test_dtypes_and_mappings(raw_record: dict) -> None:
    df = to_dataframe([raw_record])

    assert pd.api.types.is_datetime64_any_dtype(df["fecha_de_inicio_del_contrato"])
    assert pd.api.types.is_datetime64_any_dtype(df["ultima_actualizacion"])
    assert df["valor_del_contrato"].dtype == "float64"
    assert df["valor_del_contrato"].iloc[0] == 39_720_000.0
    assert df["es_pyme"].dtype == "boolean"
    assert bool(df["es_pyme"].iloc[0]) is True
    assert bool(df["es_grupo"].iloc[0]) is False
    assert isinstance(df["departamento"].dtype, pd.CategoricalDtype)


def test_url_struct_is_flattened(raw_record: dict) -> None:
    df = to_dataframe([raw_record])
    assert df["urlproceso"].iloc[0] == "https://example.test/x"


def test_doc_type_column_is_not_coerced_to_boolean(raw_record: dict) -> None:
    """El transform viejo mapeaba estas columnas a NaN; no deben ser booleanas."""
    df = to_dataframe([raw_record])
    assert df["tipo_de_documento_supervisor"].iloc[0] == "Cédula de Ciudadanía"


def test_out_of_range_dates_become_nat(raw_record: dict) -> None:
    bad = {**raw_record, "fecha_de_inicio_del_contrato": "1901-01-01T00:00:00.000"}
    df = to_dataframe([bad])
    assert pd.isna(df["fecha_de_inicio_del_contrato"].iloc[0])


def test_missing_columns_are_tolerated() -> None:
    df = to_dataframe([{"id_contrato": "X", "ultima_actualizacion": "2026-01-01"}])
    assert list(df["id_contrato"]) == ["X"]
