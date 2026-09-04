"""Contrato de esquema del dataset SECOP II.

Centraliza la clasificación de columnas que antes vivía hardcodeada en
``src/transform.py``. Todas las listas son tolerantes a *schema drift*: el
transformador sólo actúa sobre las columnas que existan en cada lote.
"""

from __future__ import annotations

# Versión del contrato. Se registra en ``_manifest.json`` en cada corrida para
# poder detectar cambios de esquema a lo largo del tiempo.
SCHEMA_VERSION = "2026.09"

# Columnas de fecha -> datetime64[ns] (UTC implícito, sin tz para compatibilidad
# con Parquet + Excel).
DATE_COLUMNS: tuple[str, ...] = (
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "ultima_actualizacion",
    "fecha_de_notificaci_n_de_prorrogaci_n",
    "fecha_inicio_liquidacion",
    "fecha_fin_liquidacion",
)

# Rango seguro para fechas: fuera de esto -> NaT (evita OutOfBoundsDatetime y
# los "años basura" tipo 2027/2030/2035 que aparecen en el origen).
DATE_MIN = "2000-01-01"
DATE_MAX = "2100-12-31"

# Columnas numéricas (llegan como string desde Socrata) -> float64.
NUMERIC_COLUMNS: tuple[str, ...] = (
    "valor_del_contrato",
    "valor_de_pago_adelantado",
    "valor_facturado",
    "valor_pendiente_de_pago",
    "valor_pagado",
    "valor_amortizado",
    "valor_pendiente_de",
    "valor_pendiente_de_ejecucion",
    "saldo_cdp",
    "saldo_vigencia",
    "dias_adicionados",
    "presupuesto_general_de_la_nacion_pgn",
    "sistema_general_de_participaciones",
    "sistema_general_de_regal_as",
    "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_",
    "recursos_de_credito",
    "recursos_propios",
)

# Columnas booleanas REALES (valores "Si" / "No" en el origen) -> pandas boolean.
# OJO: ``tipo_de_documento_ordenador_del_gasto`` y ``tipo_de_documento_supervisor``
# NO son booleanas (son "Cédula de Ciudadanía", etc.); el transform viejo las
# corrompía mapeándolas a NaN.
BOOLEAN_COLUMNS: tuple[str, ...] = (
    "es_grupo",
    "es_pyme",
    "habilita_pago_adelantado",
    "liquidaci_n",
    "obligaci_n_ambiental",
    "obligaciones_postconsumo",
    "reversion",
    "espostconflicto",
    "el_contrato_puede_ser_prorrogado",
    "documentos_tipo",
)
BOOLEAN_TRUE = "Si"
BOOLEAN_FALSE = "No"

# Columnas de baja cardinalidad -> category (ahorra memoria y acelera filtros).
CATEGORICAL_COLUMNS: tuple[str, ...] = (
    "departamento",
    "ciudad",
    "localizaci_n",
    "orden",
    "sector",
    "rama",
    "entidad_centralizada",
    "estado_contrato",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "justificacion_modalidad_de",
    "condiciones_de_entrega",
    "tipodocproveedor",
    "origen_de_los_recursos",
    "destino_gasto",
    "puntos_del_acuerdo",
    "pilares_del_acuerdo",
    "nacionalidad_representante_legal",
    "g_nero_representante_legal",
    "tipo_de_cuenta",
)

# Columnas que Socrata entrega como objeto anidado {"url": "..."} -> se aplanan
# al valor de "url".
URL_STRUCT_COLUMNS: tuple[str, ...] = ("urlproceso",)

# Columnas usadas por la CLI para filtrar (se documentan aquí como referencia).
COLUMN_NIT_ENTIDAD = "nit_entidad"
COLUMN_NIT_PROVEEDOR = "documento_proveedor"
COLUMN_NOMBRE_ENTIDAD = "nombre_entidad"
COLUMN_NOMBRE_PROVEEDOR = "proveedor_adjudicado"
COLUMN_DEPARTAMENTO = "departamento"
COLUMN_ESTADO = "estado_contrato"
COLUMN_MODALIDAD = "modalidad_de_contratacion"
COLUMN_VALOR = "valor_del_contrato"
