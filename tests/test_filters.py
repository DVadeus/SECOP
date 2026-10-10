"""Pruebas de secop.query.filters (construcción de SQL parametrizado)."""

from __future__ import annotations

import pytest

from secop.query.filters import (
    InvalidColumnError,
    QuerySpec,
    build_history_query,
    build_query,
)


def test_no_filters_selects_all() -> None:
    sql, params = build_query(QuerySpec())
    assert sql.strip() == "SELECT * FROM contratos"
    assert params == []


def test_nit_filters_are_parametrized() -> None:
    sql, params = build_query(
        QuerySpec(nit_proveedor=["900", "901"], nit_entidad=["800"])
    )
    assert "documento_proveedor IN (?, ?)" in sql
    assert "nit_entidad IN (?)" in sql
    assert params == ["900", "901", "800"]


def test_contains_uses_ilike_with_wildcards() -> None:
    sql, params = build_query(QuerySpec(entidad_contains="bogotá"))
    assert "nombre_entidad ILIKE ?" in sql
    assert params == ["%bogotá%"]


def test_year_and_value_ranges() -> None:
    sql, params = build_query(
        QuerySpec(year_from=2024, year_to=2025, valor_min=1_000_000.0)
    )
    assert "TRY_CAST(year AS INTEGER) >= ?" in sql
    assert "TRY_CAST(year AS INTEGER) <= ?" in sql
    assert "TRY_CAST(valor_del_contrato AS DOUBLE) >= ?" in sql
    assert params == [2024, "unknown", 2025, "unknown", 1_000_000.0]


def test_year_filters_never_exclude_unknown_partition() -> None:
    """year=unknown (fecha nula o fuera de rango) debe sobrevivir el filtro de año:
    no se puede descartar por año un contrato cuyo año no se conoce."""
    sql, params = build_query(QuerySpec(year_from=2024))
    assert "OR year = ?" in sql
    assert "unknown" in params

    sql, params = build_query(QuerySpec(year_from=2024, year_to=2025))
    assert sql.count("OR year = ?") == 2
    assert params.count("unknown") == 2


def test_limit_adds_order_and_param() -> None:
    sql, params = build_query(QuerySpec(limit=10))
    assert "ORDER BY ultima_actualizacion DESC" in sql
    assert sql.strip().endswith("LIMIT ?")
    assert params == [10]


def test_projection_columns_are_validated() -> None:
    sql, _ = build_query(QuerySpec(columns=["id_contrato", "valor_del_contrato"]))
    assert sql.startswith("SELECT id_contrato, valor_del_contrato FROM contratos")


def test_injection_attempt_in_column_is_rejected() -> None:
    with pytest.raises(InvalidColumnError):
        build_query(QuerySpec(columns=["1; DROP TABLE contratos; --"]))


def test_injection_attempt_in_value_stays_a_param() -> None:
    sql, params = build_query(QuerySpec(nit_proveedor=["' OR 1=1 --"]))
    assert "' OR 1=1" not in sql
    assert params == ["' OR 1=1 --"]


def test_history_query_requires_a_nit() -> None:
    with pytest.raises(ValueError):
        build_history_query([], [])


def test_history_query_combines_nits_with_or() -> None:
    sql, params = build_history_query(["11"], ["800"])
    assert "FROM contratos_historial" in sql
    assert "documento_proveedor IN (?) OR nit_entidad IN (?)" in sql
    assert sql.rstrip().endswith("ORDER BY id_contrato, historial_detectado_utc")
    assert params == ["11", "800"]
