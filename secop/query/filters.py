"""Construcción de SQL parametrizado a partir de las opciones de la CLI.

Todos los valores de usuario van como parámetros ``?`` (binding de DuckDB); nunca
se interpolan en el texto SQL. Los nombres de columna del ``--columns`` se validan
contra un patrón de identificador.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from secop.lake import UNKNOWN_YEAR
from secop.query.engine import VIEW_NAME

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class InvalidColumnError(ValueError):
    """Se pidió una columna con un nombre que no es un identificador válido."""


@dataclass(slots=True)
class QuerySpec:
    """Filtros de ``secop query`` (todos opcionales, se combinan con AND)."""

    nit_proveedor: list[str] = field(default_factory=list)
    nit_entidad: list[str] = field(default_factory=list)
    entidad_contains: str | None = None
    proveedor_contains: str | None = None
    departamento: list[str] = field(default_factory=list)
    estado: list[str] = field(default_factory=list)
    modalidad: list[str] = field(default_factory=list)
    year_from: int | None = None
    year_to: int | None = None
    valor_min: float | None = None
    valor_max: float | None = None
    columns: list[str] = field(default_factory=list)
    limit: int | None = None

    def validated_columns(self) -> list[str]:
        for col in self.columns:
            if not _IDENTIFIER.match(col):
                raise InvalidColumnError(col)
        return list(self.columns)


def _in_clause(column: str, values: list[str], params: list[object]) -> str:
    placeholders = ", ".join("?" for _ in values)
    params.extend(values)
    return f"{column} IN ({placeholders})"


def _in_clause_ci(column: str, values: list[str], params: list[object]) -> str:
    placeholders = ", ".join("?" for _ in values)
    params.extend(v.lower() for v in values)
    return f"lower({column}) IN ({placeholders})"


def build_query(spec: QuerySpec) -> tuple[str, list[object]]:
    """Devuelve ``(sql, params)`` para el ``QuerySpec`` dado."""
    cols = spec.validated_columns()
    select = ", ".join(cols) if cols else "*"

    where: list[str] = []
    params: list[object] = []

    if spec.nit_proveedor:
        where.append(_in_clause("documento_proveedor", spec.nit_proveedor, params))
    if spec.nit_entidad:
        where.append(_in_clause("nit_entidad", spec.nit_entidad, params))
    if spec.entidad_contains:
        where.append("nombre_entidad ILIKE ?")
        params.append(f"%{spec.entidad_contains}%")
    if spec.proveedor_contains:
        where.append("proveedor_adjudicado ILIKE ?")
        params.append(f"%{spec.proveedor_contains}%")
    if spec.departamento:
        where.append(_in_clause_ci("departamento", spec.departamento, params))
    if spec.estado:
        where.append(_in_clause_ci("estado_contrato", spec.estado, params))
    if spec.modalidad:
        where.append(_in_clause_ci("modalidad_de_contratacion", spec.modalidad, params))
    # "year=unknown" (fecha de inicio nula o fuera de rango) siempre pasa el
    # filtro de año: si no se conoce el año no se puede afirmar que esté fuera
    # del rango pedido, y es importante no perder esos contratos de la búsqueda.
    if spec.year_from is not None:
        where.append("(TRY_CAST(year AS INTEGER) >= ? OR year = ?)")
        params.append(spec.year_from)
        params.append(UNKNOWN_YEAR)
    if spec.year_to is not None:
        where.append("(TRY_CAST(year AS INTEGER) <= ? OR year = ?)")
        params.append(spec.year_to)
        params.append(UNKNOWN_YEAR)
    if spec.valor_min is not None:
        where.append("TRY_CAST(valor_del_contrato AS DOUBLE) >= ?")
        params.append(spec.valor_min)
    if spec.valor_max is not None:
        where.append("TRY_CAST(valor_del_contrato AS DOUBLE) <= ?")
        params.append(spec.valor_max)

    sql = f"SELECT {select} FROM {VIEW_NAME}"
    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)
    if spec.limit is not None:
        sql += "\nORDER BY ultima_actualizacion DESC NULLS LAST\nLIMIT ?"
        params.append(spec.limit)
    return sql, params
