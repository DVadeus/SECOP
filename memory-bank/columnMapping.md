# Column Mapping: interadministrativos → contratos

Measured on 2026-10-12 over the 53,445 SECOP II agreements present in both bases (join
on `id_contrato`, 1:1 in SECOP II). Implemented as `INTERADMINISTRATIVOS.column_pairs`
in `secop/bases.py`; the CLI filters use it to translate contratos column names.

## Identifiers (100 % equal) — merged into the contratos column

| interadministrativos | contratos |
|---|---|
| `id_contrato` | `id_contrato` (join key) |
| `id_proceso` | `proceso_de_compra` (in SECOP I = `id_contrato`) |
| `numero_de_contrato` | `referencia_del_contrato` |
| `link` (url) | `urlproceso` |
| `id_entidad` | `codigo_entidad` |
| `identificacion_contratista` | `documento_proveedor` |

## Same data (98.8–100 %) — merged

`nombre_entidad`→`nombre_entidad` (99.4 %), `orden`→`orden`,
`departamento`→`departamento`, `municipio`→`ciudad`,
`contratista`→`proveedor_adjudicado` (98.8 %),
`modalidad_contratacion`→`modalidad_de_contratacion`,
`tipo_de_contrato`→`tipo_de_contrato`,
`justificacion_modalidad`→`justificacion_modalidad_de`,
`estado_contrato`→`estado_contrato` (99.7 %),
`origen_de_los_recursos`→`origen_de_los_recursos`,
`objeto_contractual`→`objeto_del_contrato`,
`valor_con_adiciones`→`valor_del_contrato`,
`fecha_firma`→`fecha_de_firma` (99.9 %),
`fecha_inicio_del_contrato`→`fecha_de_inicio_del_contrato`,
`fecha_fin_del_contrato`→`fecha_de_fin_del_contrato` (99.9 %).

## Same concept, different encoding (decided with the user)

| interadministrativos | Decision |
|---|---|
| `tipo_contratista` | **merged** into `tipodocproveedor` with its original value (SECOP II is "No Definido" in 98 %; SECOP I has "Nit de Persona Jurídica", "Cédula de Ciudadanía"…) |
| `unspsc_id_clase` | **not mapped**. Equals `codigo_de_categoria_principal` in SECOP II (99.4 %), but in SECOP I it is the class *name*. In the combined view it becomes `unspsc_clase_nombre` (SECOP I only). |

## Derivable — omitted from the combined view

`anno_firma` = `year(fecha_de_firma)` (99.9 %), `anno_cargue` = `year(fecha_cargue)`.

## New — the only columns the combined view adds

| Column | Note |
|---|---|
| `fecha_cargue` | Load date into the dataset; not `ultima_actualizacion` (0.2 % match) |
| `fuente` → `plataforma` | `SECOP_I`/`SECOP_II`; contratos is always `SECOP_II` |
| `unspsc_id_clase` (SECOP I) → `unspsc_clase_nombre` | See above |

Computed: `base_origen` (`contratos`|`interadministrativos`),
`en_base_interadministrativos` (id present in `s484-c9k3`), `es_interadministrativo`
(`justificacion_modalidad_de ILIKE '%interadministrativ%'` OR present in `s484-c9k3`).
Both flags are needed: contratos has 127,146 contracts justified as interadministrative
agreements and only 53,445 (42 %) are in `s484-c9k3`, which starts with loads from
Dec 2023.

## Combined view `contratos_todos` — design for a later PR (not implemented)

- Part A: all `contratos` rows + `LEFT JOIN` 1:1 on `id_contrato` with
  interadministrativos (SECOP II), bringing only `fecha_cargue`, `plataforma` and the
  flags. For overlapping contracts the contratos values win (updated by version);
  differences (contractor name 1.2 %, entity name 0.6 %, state 0.3 %) can be audited by
  comparing the two views.
- Part B (`UNION ALL BY NAME`): interadministrativos rows whose `id_contrato` is not in
  contratos (~33.7 k SECOP I rows, a few SECOP II), columns renamed with the mapping and
  contratos-only columns NULL.
- Grain: SECOP I may have several rows per contract → count with
  `count(DISTINCT id_contrato)`.
