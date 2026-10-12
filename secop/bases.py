"""Bases de datos (datasets de datos.gov.co) que carga el ETL.

Cada :class:`Base` reúne lo que distingue a un dataset: su id en Socrata, dónde
vive en el lake, su clave, su esquema y cómo se detectan sus cambios. Todas las
funciones del ETL y de la consulta reciben una ``base`` (por defecto
:data:`CONTRATOS`, la base original), así que agregar un dataset no cambia el
comportamiento de los existentes.
"""

from __future__ import annotations

from dataclasses import dataclass

from secop import schema
from secop.config import (
    DATA_FILE,
    DATASET_ID,
    HISTORY_PREFIX,
    INDEX_KEY,
    LAKE_PREFIX,
    MANIFEST_KEY,
    PARTITION_DATE_COLUMN,
    PRIMARY_KEY,
    RUN_MARKER_KEY,
    STATE_KEY,
    WATERMARK_COLUMN,
)

_SOCRATA = "https://www.datos.gov.co"

#: Foto ligera del origen + índice de ids (``secop.diff``).
STRATEGY_SNAPSHOT_DIFF = "snapshot_diff"
#: Bajar el dataset completo en cada corrida (datasets pequeños sin columna de
#: versión); el hash de contenido decide qué cambió.
STRATEGY_FULL_REFRESH = "full_refresh"


@dataclass(frozen=True, slots=True)
class SchemaSpec:
    """Clasificación de columnas para tipar los registros crudos de Socrata."""

    date_columns: tuple[str, ...] = ()
    numeric_columns: tuple[str, ...] = ()
    boolean_columns: tuple[str, ...] = ()
    categorical_columns: tuple[str, ...] = ()
    #: Columnas que Socrata entrega como ``{"url": "..."}``.
    url_struct_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Base:
    """Un dataset de datos.gov.co y su lugar en el lake."""

    #: Nombre en la CLI y en las vistas de DuckDB.
    name: str
    dataset_id: str
    #: Archivo de cada partición ``year=*`` (y de cada día del historial).
    data_file: str
    lake_prefix: str
    history_prefix: str
    state_key: str
    index_key: str
    manifest_key: str
    run_marker_key: str
    #: Identificador del contrato.
    key: str
    #: Columna de versión (mayor = más nueva), o ``None`` si el dataset no tiene.
    version_column: str | None
    partition_date_column: str
    schema: SchemaSpec
    strategy: str = STRATEGY_SNAPSHOT_DIFF
    #: Un contrato puede tener varias filas (p.ej. una por parte o por origen de
    #: recursos): la unidad de versión y de partición es el contrato completo.
    grouped: bool = False
    #: Pares (columna propia, columna equivalente de la base de contratos).
    column_pairs: tuple[tuple[str, str], ...] = ()
    #: Columna para "lo más reciente primero" (``--limit``).
    recency_column: str = WATERMARK_COLUMN
    #: Columna con la plataforma SECOP_I/SECOP_II (``--plataforma``), si existe.
    platform_column: str | None = None

    @property
    def column_map(self) -> dict[str, str]:
        """Columna propia -> columna equivalente de contratos."""
        return dict(self.column_pairs)

    @property
    def resource_url(self) -> str:
        return f"{_SOCRATA}/resource/{self.dataset_id}.json"

    @property
    def metadata_url(self) -> str:
        return f"{_SOCRATA}/api/views/{self.dataset_id}.json"

    @property
    def history_view(self) -> str:
        """Nombre de la vista de historial en DuckDB."""
        return f"{self.name}_historial"


#: Contratos SECOP II (``jbjy-vk9h``): la base original, sin cambios de rutas.
CONTRATOS = Base(
    name="contratos",
    dataset_id=DATASET_ID,
    data_file=DATA_FILE,
    lake_prefix=LAKE_PREFIX,
    history_prefix=HISTORY_PREFIX,
    state_key=STATE_KEY,
    index_key=INDEX_KEY,
    manifest_key=MANIFEST_KEY,
    run_marker_key=RUN_MARKER_KEY,
    key=PRIMARY_KEY,
    version_column=WATERMARK_COLUMN,
    partition_date_column=PARTITION_DATE_COLUMN,
    schema=SchemaSpec(
        date_columns=schema.DATE_COLUMNS,
        numeric_columns=schema.NUMERIC_COLUMNS,
        boolean_columns=schema.BOOLEAN_COLUMNS,
        categorical_columns=schema.CATEGORICAL_COLUMNS,
        url_struct_columns=schema.URL_STRUCT_COLUMNS,
    ),
)

#: Convenios interadministrativos SECOP I y II (``s484-c9k3``). Comparte las
#: carpetas ``year=*`` con contratos, con su propio archivo. Sin columna de
#: versión y con varias filas por contrato en SECOP I (una por parte o por
#: ``origen_de_los_recursos``): se baja completo y se versiona por contrato.
INTERADMINISTRATIVOS = Base(
    name="interadministrativos",
    dataset_id="s484-c9k3",
    data_file="interadministrativos.parquet",
    lake_prefix=LAKE_PREFIX,
    history_prefix=HISTORY_PREFIX,
    state_key="state/interadministrativos.json",
    index_key="state/interadministrativos_id_index.parquet",
    manifest_key="_manifest_interadministrativos.json",
    run_marker_key="state/interadministrativos_run_in_progress.json",
    key=PRIMARY_KEY,
    version_column=None,
    partition_date_column="fecha_inicio_del_contrato",
    schema=SchemaSpec(
        date_columns=(
            "fecha_cargue",
            "fecha_firma",
            "fecha_inicio_del_contrato",
            "fecha_fin_del_contrato",
        ),
        numeric_columns=("valor_con_adiciones", "anno_cargue", "anno_firma"),
        categorical_columns=(
            "orden",
            "departamento",
            "municipio",
            "modalidad_contratacion",
            "tipo_de_contrato",
            "justificacion_modalidad",
            "estado_contrato",
            "fuente",
            "tipo_contratista",
            "origen_de_los_recursos",
        ),
        url_struct_columns=("link",),
    ),
    strategy=STRATEGY_FULL_REFRESH,
    grouped=True,
    # Equivalencias medidas en los 53.445 convenios presentes en ambas bases
    # (2026-10-12). unspsc_id_clase NO se mapea: en SECOP I es el nombre de la
    # clase, no el código de codigo_de_categoria_principal.
    column_pairs=(
        # Identificadores (100 %).
        ("id_contrato", "id_contrato"),
        ("id_proceso", "proceso_de_compra"),
        ("numero_de_contrato", "referencia_del_contrato"),
        ("link", "urlproceso"),
        ("id_entidad", "codigo_entidad"),
        ("identificacion_contratista", "documento_proveedor"),
        # Mismo dato (98,8-100 %).
        ("nombre_entidad", "nombre_entidad"),
        ("orden", "orden"),
        ("departamento", "departamento"),
        ("municipio", "ciudad"),
        ("contratista", "proveedor_adjudicado"),
        ("modalidad_contratacion", "modalidad_de_contratacion"),
        ("tipo_de_contrato", "tipo_de_contrato"),
        ("justificacion_modalidad", "justificacion_modalidad_de"),
        ("estado_contrato", "estado_contrato"),
        ("origen_de_los_recursos", "origen_de_los_recursos"),
        ("objeto_contractual", "objeto_del_contrato"),
        ("valor_con_adiciones", "valor_del_contrato"),
        ("fecha_firma", "fecha_de_firma"),
        ("fecha_inicio_del_contrato", "fecha_de_inicio_del_contrato"),
        ("fecha_fin_del_contrato", "fecha_de_fin_del_contrato"),
        # Mismo concepto, otro vocabulario: se funde con su valor original.
        ("tipo_contratista", "tipodocproveedor"),
    ),
    recency_column="fecha_cargue",
    platform_column="fuente",
)

BASES: dict[str, Base] = {b.name: b for b in (CONTRATOS, INTERADMINISTRATIVOS)}


def get_base(name: str) -> Base:
    """Base por nombre (como se escribe en la CLI)."""
    try:
        return BASES[name]
    except KeyError:
        valid = ", ".join(BASES)
        raise ValueError(f"base desconocida: {name!r} (usa {valid})") from None
