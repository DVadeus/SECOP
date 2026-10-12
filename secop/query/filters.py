"""Construcción de SQL parametrizado a partir de las opciones de la CLI.

Todos los valores de usuario van como parámetros ``?`` (binding de DuckDB); nunca
se interpolan en el texto SQL. Los nombres de columna del ``--columns`` se validan
contra un patrón de identificador.

Los filtros se expresan con los nombres de columna de la base de contratos y se
traducen a la base consultada con su ``column_map`` (ver ``secop.bases``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from secop.bases import CONTRATOS, Base
from secop.lake import UNKNOWN_YEAR

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class InvalidColumnError(ValueError):
    """Se pidió una columna con un nombre que no es un identificador válido."""


class UnsupportedFilterError(ValueError):
    """La base consultada no tiene una columna para ese filtro."""


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
    plataforma: list[str] = field(default_factory=list)
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


def column_for(base: Base, contratos_column: str, option: str) -> str:
    """Columna de ``base`` equivalente a ``contratos_column`` de contratos."""
    if not base.column_map:
        return contratos_column
    inverse = {target: own for own, target in base.column_map.items()}
    if contratos_column not in inverse:
        raise UnsupportedFilterError(f"{option} no está disponible en {base.name}")
    return inverse[contratos_column]


def _in_clause(column: str, values: list[str], params: list[object]) -> str:
    placeholders = ", ".join("?" for _ in values)
    params.extend(values)
    return f"{column} IN ({placeholders})"


def _in_clause_ci(column: str, values: list[str], params: list[object]) -> str:
    placeholders = ", ".join("?" for _ in values)
    params.extend(v.lower() for v in values)
    return f"lower({column}) IN ({placeholders})"


def _nit_entidad_clause(base: Base, nits: list[str], params: list[object]) -> str:
    """Filtro por NIT de entidad, también en bases que no lo tienen.

    En interadministrativos no hay NIT de entidad, pero ``id_entidad`` es el
    ``codigo_entidad`` de contratos (100 % en SECOP II): se resuelve con la
    vista ``contratos``. Las entidades de SECOP I no tienen esa equivalencia.
    """
    if not base.column_map or "nit_entidad" in base.column_map.values():
        return _in_clause(
            column_for(base, "nit_entidad", "--nit-entidad"), nits, params
        )
    own = column_for(base, "codigo_entidad", "--nit-entidad")
    inner = _in_clause("nit_entidad", nits, params)
    return (
        f"{own} IN (SELECT DISTINCT CAST(codigo_entidad AS VARCHAR) "
        f"FROM {CONTRATOS.name} WHERE {inner})"
    )


def _text_filters(spec: QuerySpec, base: Base, params: list[object]) -> list[str]:
    where: list[str] = []
    if spec.nit_proveedor:
        col = column_for(base, "documento_proveedor", "--nit-proveedor")
        where.append(_in_clause(col, spec.nit_proveedor, params))
    if spec.nit_entidad:
        where.append(_nit_entidad_clause(base, spec.nit_entidad, params))
    if spec.entidad_contains:
        where.append(
            f"{column_for(base, 'nombre_entidad', '--entidad-contains')} ILIKE ?"
        )
        params.append(f"%{spec.entidad_contains}%")
    if spec.proveedor_contains:
        col = column_for(base, "proveedor_adjudicado", "--proveedor-contains")
        where.append(f"{col} ILIKE ?")
        params.append(f"%{spec.proveedor_contains}%")
    for values, column, option in (
        (spec.departamento, "departamento", "--departamento"),
        (spec.estado, "estado_contrato", "--estado"),
        (spec.modalidad, "modalidad_de_contratacion", "--modalidad"),
    ):
        if values:
            where.append(
                _in_clause_ci(column_for(base, column, option), values, params)
            )
    if spec.plataforma:
        if base.platform_column is None:
            raise UnsupportedFilterError(
                f"--plataforma no está disponible en {base.name}"
            )
        where.append(_in_clause_ci(base.platform_column, spec.plataforma, params))
    return where


def _range_filters(spec: QuerySpec, base: Base, params: list[object]) -> list[str]:
    where: list[str] = []
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
    if spec.valor_min is not None or spec.valor_max is not None:
        valor = column_for(base, "valor_del_contrato", "--valor-min/--valor-max")
        if spec.valor_min is not None:
            where.append(f"TRY_CAST({valor} AS DOUBLE) >= ?")
            params.append(spec.valor_min)
        if spec.valor_max is not None:
            where.append(f"TRY_CAST({valor} AS DOUBLE) <= ?")
            params.append(spec.valor_max)
    return where


def build_query(spec: QuerySpec, base: Base = CONTRATOS) -> tuple[str, list[object]]:
    """Devuelve ``(sql, params)`` para el ``QuerySpec`` dado sobre ``base``."""
    cols = spec.validated_columns()
    select = ", ".join(cols) if cols else "*"

    params: list[object] = []
    where = _text_filters(spec, base, params) + _range_filters(spec, base, params)

    sql = f"SELECT {select} FROM {base.name}"
    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)
    if spec.limit is not None:
        sql += f"\nORDER BY {base.recency_column} DESC NULLS LAST\nLIMIT ?"
        params.append(spec.limit)
    return sql, params


def build_history_query(
    nit_proveedor: list[str], nit_entidad: list[str], base: Base = CONTRATOS
) -> tuple[str, list[object]]:
    """``(sql, params)`` de ``secop historial``: versiones reemplazadas por NIT.

    Exige al menos un NIT. Si se dan ambos tipos se combinan con OR (todas las
    versiones en que participa cualquiera de los NIT), ordenadas por contrato y
    fecha de detección.
    """
    if not nit_proveedor and not nit_entidad:
        raise ValueError("Indica al menos un --nit-proveedor o --nit-entidad")
    clauses: list[str] = []
    params: list[object] = []
    if nit_proveedor:
        col = column_for(base, "documento_proveedor", "--nit-proveedor")
        clauses.append(_in_clause(col, nit_proveedor, params))
    if nit_entidad:
        clauses.append(_nit_entidad_clause(base, nit_entidad, params))
    sql = (
        f"SELECT * FROM {base.history_view}\nWHERE "
        + " OR ".join(clauses)
        + "\nORDER BY id_contrato, historial_detectado_utc"
    )
    return sql, params
