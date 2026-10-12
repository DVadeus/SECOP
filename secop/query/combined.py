"""Vista combinada ``contratos_todos``: ambas bases en una sola consulta.

Diseño y mapeo medido en ``memory-bank/columnMapping.md``:

* **Parte A**: todas las filas de ``contratos`` (sus columnas, sin cambios) más un
  ``LEFT JOIN`` 1:1 por ``id_contrato`` con los convenios que también están en
  ``interadministrativos``. De esa base sólo se agregan las columnas nuevas.
* **Parte B** (``UNION ALL BY NAME``): filas de ``interadministrativos`` cuyo
  ``id_contrato`` no está en contratos (sobre todo SECOP I), con sus columnas
  renombradas a las de contratos según ``INTERADMINISTRATIVOS.column_map``. Las
  columnas exclusivas de contratos quedan en NULL.

Columnas que agrega la vista:

* ``base_origen``: ``contratos`` o ``interadministrativos`` (de dónde sale la fila).
* ``plataforma``: ``SECOP_I``/``SECOP_II`` (columna ``fuente``; contratos = SECOP_II).
* ``fecha_cargue``: fecha de carga en el dataset de convenios.
* ``unspsc_clase_nombre``: el UNSPSC de SECOP I, que es un nombre de clase y no
  el código de ``codigo_de_categoria_principal``.
* ``en_base_interadministrativos``: el contrato está en ``s484-c9k3``.
* ``es_interadministrativo``: justificación de convenio interadministrativo en
  contratos, o presencia en ``s484-c9k3``.

La vista no completa datos que el origen no publica (p.ej. ``nit_entidad`` queda
NULL en la parte B). En SECOP I un contrato puede tener varias filas: para contar
contratos usar ``count(DISTINCT id_contrato)``.
"""

from __future__ import annotations

from dataclasses import dataclass

from secop.bases import CONTRATOS, INTERADMINISTRATIVOS

COMBINED_VIEW = "contratos_todos"

COL_BASE_ORIGEN = "base_origen"
COL_PLATAFORMA = "plataforma"
COL_FECHA_CARGUE = "fecha_cargue"
COL_UNSPSC_NOMBRE = "unspsc_clase_nombre"
COL_EN_BASE = "en_base_interadministrativos"
COL_ES_INTER = "es_interadministrativo"

_JUSTIFICACION = "justificacion_modalidad_de"
_UNSPSC = "unspsc_id_clase"
_CODIGO_CATEGORIA = "codigo_de_categoria_principal"
_FUENTE = "fuente"
_SECOP_II = "SECOP_II"


@dataclass(frozen=True, slots=True)
class CombinedView:
    """Destino de consulta ``--base todas`` (vista, no dataset: no tiene ETL)."""

    name: str = COMBINED_VIEW
    #: Contratos tiene ``ultima_actualizacion``; las filas de la parte B, sólo
    #: ``fecha_cargue``.
    recency_column: str = f"coalesce(ultima_actualizacion, {COL_FECHA_CARGUE})"
    platform_column: str = COL_PLATAFORMA
    partition_date_column: str = CONTRATOS.partition_date_column

    @property
    def column_map(self) -> dict[str, str]:
        """La vista ya usa los nombres de contratos: no hay traducción."""
        return {}


TODAS = CombinedView()


def _or_null(columns: set[str], expr: str, column: str, cast: str = "VARCHAR") -> str:
    """``expr`` si ``column`` existe en la vista, si no un NULL tipado."""
    return expr if column in columns else f"CAST(NULL AS {cast})"


def _part_a(contratos_columns: set[str], inter_columns: set[str]) -> str:
    fecha_cargue = _or_null(
        inter_columns, f"max({COL_FECHA_CARGUE})", COL_FECHA_CARGUE, "TIMESTAMP"
    )
    justificacion = _or_null(
        contratos_columns,
        f"c.{_JUSTIFICACION} ILIKE '%interadministrativ%'",
        _JUSTIFICACION,
        "BOOLEAN",
    )
    key = CONTRATOS.key
    return f"""
SELECT
    c.*,
    '{CONTRATOS.name}' AS {COL_BASE_ORIGEN},
    '{_SECOP_II}' AS {COL_PLATAFORMA},
    ia.{COL_FECHA_CARGUE},
    CAST(NULL AS VARCHAR) AS {COL_UNSPSC_NOMBRE},
    ia.ia_id IS NOT NULL AS {COL_EN_BASE},
    (coalesce({justificacion}, false) OR ia.ia_id IS NOT NULL) AS {COL_ES_INTER}
FROM {CONTRATOS.name} c
LEFT JOIN (
    SELECT {INTERADMINISTRATIVOS.key} AS ia_id, {fecha_cargue} AS {COL_FECHA_CARGUE}
    FROM {INTERADMINISTRATIVOS.name}
    GROUP BY 1
) ia ON c.{key} = ia.ia_id"""


def _part_b(inter_columns: set[str]) -> str:
    renamed = [
        f"i.{own} AS {target}"
        for own, target in INTERADMINISTRATIVOS.column_map.items()
        if own in inter_columns
    ]
    has_unspsc = _UNSPSC in inter_columns and _FUENTE in inter_columns
    unspsc_code = f"CASE WHEN i.{_FUENTE} = '{_SECOP_II}' THEN i.{_UNSPSC} END"
    unspsc_name = (
        f"CASE WHEN i.{_FUENTE} IS DISTINCT FROM '{_SECOP_II}' THEN i.{_UNSPSC} END"
    )
    extra = [
        f"{unspsc_code if has_unspsc else 'CAST(NULL AS VARCHAR)'} AS {_CODIGO_CATEGORIA}",
        f"{unspsc_name if has_unspsc else 'CAST(NULL AS VARCHAR)'} AS {COL_UNSPSC_NOMBRE}",
        f"'{INTERADMINISTRATIVOS.name}' AS {COL_BASE_ORIGEN}",
        f"{_or_null(inter_columns, f'i.{_FUENTE}', _FUENTE)} AS {COL_PLATAFORMA}",
        f"{_or_null(inter_columns, f'i.{COL_FECHA_CARGUE}', COL_FECHA_CARGUE, 'TIMESTAMP')}"
        f" AS {COL_FECHA_CARGUE}",
        f"true AS {COL_EN_BASE}",
        f"true AS {COL_ES_INTER}",
        "i.year",
    ]
    select = ",\n    ".join(renamed + extra)
    key = INTERADMINISTRATIVOS.key
    return f"""
SELECT
    {select}
FROM {INTERADMINISTRATIVOS.name} i
ANTI JOIN (SELECT DISTINCT {CONTRATOS.key} AS c_id FROM {CONTRATOS.name}) k
    ON i.{key} = k.c_id"""


def combined_view_sql(contratos_columns: set[str], inter_columns: set[str]) -> str:
    """``CREATE VIEW contratos_todos`` según las columnas presentes en cada vista."""
    return (
        f"CREATE OR REPLACE VIEW {COMBINED_VIEW} AS"
        f"{_part_a(contratos_columns, inter_columns)}\n"
        f"UNION ALL BY NAME"
        f"{_part_b(inter_columns)}"
    )
