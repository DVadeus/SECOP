# CLAUDE.md

Guía para trabajar en este repositorio y registro del análisis del código.
Última revisión: 2026-10-12 (rama `feat/base-interadministrativos`).

## Qué es

ETL de dos **bases** de datos.gov.co (API Socrata) hacia un data lake Parquet en
**Cloudflare R2**, más una CLI (`secop`) que consulta el lake con **DuckDB** y exporta a
Excel/CSV/Parquet. Usuario final: la UAN del Concejo de Bogotá (reemplaza búsquedas
manuales en SECOP / PACO).

| Base (`--base`) | Dataset | Contenido |
|---|---|---|
| `contratos` (por defecto) | `jbjy-vk9h` | Contratos SECOP II, ~6,1 M filas, 95 columnas |
| `interadministrativos` | `s484-c9k3` | Convenios interadministrativos SECOP I y II, ~87 mil filas, 27 columnas |

Fuera de alcance (ver `memory-bank/productContext.md`): dashboards, ML, estadística,
KPIs. La capa de consulta sólo filtra, agrega y exporta.

Prioridades: corrección > rendimiento, fiabilidad > velocidad, reproducibilidad >
comodidad. **Nunca se borra un contrato del lake**, aunque el origen lo elimine
(decisión del usuario: SECOP es un registro público y las desapariciones son evidencia).

## Comandos

```bash
uv sync                                   # instala (Python 3.13, ver .python-version)
uv run pytest                             # excluye tests marcados 'integration'
uv run ruff check . && uv run ruff format --check .
uv run basedpyright secop                 # modo "standard"

# Todos aceptan --base contratos|interadministrativos (por defecto contratos)
uv run secop etl backfill --start 2018 --end 2026 --target r2 [--year Y --force]
uv run secop etl incremental [--target local] [--base interadministrativos]
uv run secop etl reparar [--aplicar]      # sin --aplicar sólo informa
uv run secop query --nit-entidad 900475780 --year-from 2024 -o out.xlsx [--source local --path DIR]
uv run secop query --base interadministrativos --plataforma SECOP_I -o conv.xlsx
uv run secop historial --nit-proveedor 830125075 -o versiones.xlsx
uv run secop sql "SELECT year, count(*) FROM contratos GROUP BY 1" [-o out.csv]
uv run secop info [--source local]
```

Estado: 115 tests pasan, ruff y basedpyright sin errores.

## Arquitectura

```
Socrata ─► foto ligera (id, ua, fecha_inicio) ─► diff vs índice ─► fetch_by_ids
                                                                        │
                                       transform ─► _apply_batch (reglas entre particiones)
                                                                        │
         raw/contratos/year=*/data.parquet  ◄── merge_versions ──► raw/contratos_historial/
         state/id_index.parquet · state/contratos.json · _manifest.json
                                                                        │
                    query.engine (DuckDB: vistas contratos y contratos_historial) ─► export
```

| Módulo | Responsabilidad |
|---|---|
| `secop/bases.py` | `Base` (dataclass congelada): dataset, `data_file` por partición, claves de estado/índice/manifest/marca, clave, versión, fecha de partición, `SchemaSpec`, `strategy` (`snapshot_diff`/`full_refresh`), `grouped`, `column_pairs` (mapeo a contratos), `recency_column`, `platform_column`. `CONTRATOS` (rutas originales) e `INTERADMINISTRATIVOS`. Todas las funciones reciben `base` (por defecto `CONTRATOS`). |
| `secop/config.py` | Constantes de contratos (alias de `CONTRATOS`) y `Settings` (pydantic-settings, `.env` en la raíz; `get_settings()` cacheado). |
| `secop/extract.py` | `iter_pages`/`fetch_data` (`$order=:id`, 4 reintentos, `$select` opcional). `fetch_snapshot` (foto ligera, verifica `rowsUpdatedAt` antes y después → `SourceReloadedError`). `fetch_by_ids` (`id_contrato in (...)`, 200 por petición). `fetch_all_checked` (dataset completo con la misma verificación; `full_refresh`). |
| `secop/schema.py` / `transform.py` | Clasificación de columnas y tipado de registros crudos. Nunca rellena NULLs. |
| `secop/lake.py` | Partición `year=YYYY` por `fecha_de_inicio_del_contrato` (nulo o fuera de `[2000, año+2]` → `year=unknown`). `merge_versions`: una versión por id (gana `ultima_actualizacion` mayor; empate → la recién llegada) y devuelve las perdedoras con contenido distinto. `upsert_partition` escribe historial **antes** que la partición y nunca elimina ids. `move_out_ids` saca ids de una partición pasándolos al historial. En bases `grouped` (`_merge_groups`) la versión es el contrato completo (multiconjunto de hashes de fila) y todas sus filas van a la partición del año válido más temprano. `upsert` devuelve `MergeResult` (`changed`, `applied`); si nada cambió no reescribe. |
| `secop/history.py` | Historial anexable en `raw/contratos_historial/detectado=YYYY-MM-DD/`; columnas `year`, `historial_motivo` (`version_reemplazada`, `fecha_completada`, `fecha_eliminada_en_origen`), `historial_detectado_utc`, `historial_hash`. Idempotente por hash de contenido (`None`≡`NaN`). |
| `secop/index.py` | Índice `state/id_index.parquet`: una fila por (id, partición) con su `ultima_actualizacion`. Reconstruible desde el lake (`build_from_lake`, ~30 s en R2). Da `row_counts` y el watermark informativo. |
| `secop/diff.py` | DuckDB: qué ids bajar comparando la foto con el índice (no está en su partición destino; versión más nueva; origen borró la fecha y trae versión nueva). |
| `secop/pipeline.py` | `_apply_batch` aplica las reglas entre particiones. `backfill()` (por año, reanudable, siempre fusiona), `incremental()` (foto → diff → fetch por lotes de 50.000 ids), `repair()` (saca de `unknown` los ids que ya están con fecha, con respaldo). Bases `full_refresh`: `_full_refresh` (baja todo y fusiona por contenido; `backfill` ignora años). `_begin`/`_finish` gestionan la marca, índice, estado y manifest de cada base. |
| `secop/state.py` | `PipelineState` en `state/contratos.json`. `ultima_actualizacion` es **informativo** (máximo del índice); la extracción no depende de él. |
| `secop/storage/` | Protocolo `ObjectStore` y `R2Store` (boto3) / `LocalStore` (escritura atómica). |
| `secop/query/` | `QueryEngine`: una vista por base (`contratos`, `interadministrativos`, glob `year=*/<data_file>`) y su `*_historial`, sólo si hay archivos (`has_view`/`require_view`). `filters.build_query(spec, base)` / `build_history_query`: filtros con nombres de contratos traducidos con `column_map` (`UnsupportedFilterError` si no hay columna); `--nit-entidad` en interadministrativos vía `codigo_entidad` de contratos (sólo SECOP II); `plataforma`. Exportador por extensión. |
| `secop/cli.py` | Typer: `etl backfill|incremental|reparar`, `query`, `historial`, `sql`, `info`; `--base` y `--plataforma`. |

Reglas entre particiones (`pipeline._apply_batch`):

- `year=unknown` → año real: la versión con fecha va a su año; la copia sin fecha sale
  de `unknown` al historial (`fecha_completada`).
- Año A → año B: se conservan ambas copias en el lake (evidencia); se avisa en el log.
- Año real → sin fecha en el origen: se conserva la versión con fecha; la nueva va al
  historial (`fecha_eliminada_en_origen`).

CI: `.github/workflows/etl-weekly.yml` corre `etl incremental` (contratos) y luego
`etl incremental --base interadministrativos` (`if: always()`: corre aunque falle el
primero) los lunes 06:00 UTC; cada paso con un reintento a los 15 min (p.ej. por recarga
del origen); timeout 120 min. Con `workflow_dispatch` + `year` hace
`backfill --year Y --force` de contratos (fusiona, no borra).

## Convenciones

De `memory-bank/codingStandards.md` y del propio código:

- Python ≥ 3.13, `from __future__ import annotations`, type hints en todo.
- `pathlib`, `logging` (nunca `print` fuera de la salida Rich de la CLI).
- Funciones de < 50 líneas; dataclasses (`slots=True`) para estructuras de datos.
- No capturar `Exception` genérica salvo para relanzar (única excepción: `_print_state`
  en la CLI, marcada `noqa: BLE001`).
- Docstrings y mensajes en español. Ruff (`E,F,I,UP,B`, línea 88, comillas dobles).
- Tests con `LocalStore` en `tmp_path`, origen simulado con `monkeypatch` sobre
  `pipeline.fetch_snapshot`/`fetch_by_ids` (contratos) o `pipeline.fetch_all_checked`
  (interadministrativos); nada de red. Tests contra Socrata/R2 reales llevan
  `@pytest.mark.integration`.
- Lo que distingue a un dataset va en su `Base` (`secop/bases.py`); no hardcodear
  nombres de columna ni rutas. Para agregar un dataset: definir la `Base`, registrarla
  en `BASES` y (si es otra estrategia) despacharla en `pipeline`.
- En Git Bash de Windows, los heredocs con `\\n` se convierten en saltos reales: para
  editar código con secuencias de escape usar la herramienta Edit, no scripts.

## Contrato de datos (no romper)

Detalle en `memory-bank/datasetContract.md` e `incrementalStrategy.md`.

- Nunca borrar contratos del lake. Toda versión reemplazada va al historial.
- Exactamente un archivo compactado **por base** en cada partición (`data.parquet`,
  `interadministrativos.parquet`); nunca `part-*`. Cada vista lee sólo su archivo.
- En interadministrativos la unidad es el contrato completo (varias filas en SECOP I):
  nunca partirlo entre años ni versionarlo por fila.
- Un id nunca debe estar a la vez en `year=unknown` y en un año real.
- Historial antes que partición; índice, estado y manifest al final; marca de corrida.
- Nunca rellenar NULLs. Tiempos tratados como UTC (naive en Parquet).
- `year` es string (partición Hive); usar `TRY_CAST(year AS INTEGER)`.
- Si se cambian tipos o columnas en `schema.py`, subir `SCHEMA_VERSION`.

## Hallazgos del análisis y estado

Auditoría de R2 vs origen (2026-10-09, sólo lectura) y estado tras la limpieza del
2026-10-10:

| Hallazgo | Medida | Estado |
|---|---|---|
| Origen se recarga a diario; `:updated_at`/`:id` inútiles | todas las filas con el mismo `:updated_at` | Diseño: foto + índice |
| `ultima_actualizacion` nula en el origen | 2.139.277 de 5.621.358 (38 %) | Etapa 5–6 |
| Contratos del origen ausentes en R2 (nuevos sin `ua`) | 95.467 | Cargados (incremental 2026-10-10) |
| `unknown` en R2 que ya tienen fecha en el origen | 30.869 | Movidos a su año (incremental 2026-10-10) |
| Ids en `unknown` y además en un año real | 1.916 | Movidos al historial (`etl reparar --aplicar`) |
| Ids en dos años reales | 10 | Se conservan (evidencia) |
| Ids en R2 que ya no están en el origen | 491.931 (sobre todo Borrador/Cancelado) | Se conservan (decisión) |
| Ids duplicados dentro del propio origen | 298 | `merge_versions` |

Limpieza de R2 (2026-10-10, tras integrar el PR #1):

1. Respaldo completo (copia en el servidor, verificada por tamaño y ETag) en
   `backup/20261010T062725Z/`: estado del 2026-10-05, 23 objetos, 2,12 GB. `etl
   reparar` dejó además `backup/20261010T063107Z/` (unknown, estado, manifest). Se
   conservan hasta que el usuario decida borrarlos.
2. `secop etl reparar --aplicar`: 1.916 copias de `unknown` al historial.
3. `secop etl incremental`: 139.938 ids descargados; 13.602 versiones reemplazadas al
   historial.
4. Auditoría final contra una foto nueva del origen: 0 contratos faltantes, 0 ids en
   `unknown` y en un año real, 0 pendientes; índice, manifest y `row_counts`
   coinciden con el lake (6.113.001 filas). Historial: 32.785 `fecha_completada` y
   13.602 `version_reemplazada`.

GitHub Actions: el workflow corrió con éxito todos los lunes desde 2026-09-07 (5
corridas), pero con el código viejo; los datos faltantes eran un límite del diseño, no
un fallo de ejecución. El cron (06:00 UTC) arranca con 5–7 h de retraso por la carga
de GitHub. La corrida #6 (2026-10-12, 23 min) fue la primera con el código nuevo:
6.120.107 filas, 11 contratos en dos años.

Perfil de `s484-c9k3` (2026-10-12, sólo lectura) que definió la base
interadministrativos (PR `feat/base-interadministrativos`):

- Esquema distinto al de contratos: 27 columnas, sólo 7 con el mismo nombre. Sin
  `ultima_actualizacion` ni `nit_entidad`. Mapeo medido en
  `memory-bank/columnMapping.md` (22 equivalencias; `unspsc_id_clase` no se mapea).
- 87.254 filas: 33.735 de SECOP I y 53.519 de SECOP II. En SECOP I un contrato tiene
  una fila por parte o por `origen_de_los_recursos` (+23 filas idénticas): no hay clave
  por fila, la unidad es el contrato (85.852 contratos, 87.231 filas al cargar).
- El 95,5 % de su SECOP II ya está en contratos; contratos tiene 127.146 contratos
  justificados como convenio interadministrativo, sólo el 42 % está en `s484-c9k3`.
- La vista combinada `contratos_todos` está diseñada en `columnMapping.md` y queda para
  un PR posterior (decisión del usuario).
- Primera carga en R2 (2026-10-12, desde la rama, antes de integrar el PR: el código
  de `main` sólo lee `*/data.parquet`): 85.852 contratos en 87.231 filas (33.712
  SECOP I, 53.519 SECOP II), 28 particiones, ~19 MB. Antes/después se compararon los
  ETag de los 52 objetos previos: ninguno cambió (31 objetos nuevos). Una segunda
  corrida escribió 0 contratos y sólo reescribió estado y manifest.

Defectos del código y etapa que los corrigió (commits del PR #1, `fix/integridad-lake`):

1. Lake local / glob leía `part-*` del ETL viejo; README y notebook usaban `data/raw` →
   **Etapa 1**.
2. Historial de versiones (requisito nuevo) → **Etapa 2**.
3. Duplicados entre particiones; backfill `--force` sobrescribía (borraba contratos);
   `compact` global descartaba copias en años distintos → **Etapa 3**.
4. `row_counts` incompleto en backfill reanudado → **Etapa 4**.
5. Contratos sin `ultima_actualizacion` invisibles al incremental → **Etapa 5**.
6. Watermark inseguro (un backfill reanudado podía adelantarlo y perder cambios de
   años omitidos; no "seguro", como decía la primera versión de este análisis) →
   **Etapa 6**: la extracción ya no depende del watermark.
7. Costo: `unknown` (136 MB) se leía en cada lote → **Etapa 7** (índice fiable con
   marca de corrida).
8. Datos ya escritos en R2 → **Etapa 8** (`etl reparar`).
9. El resumen de la corrida sumaba el tamaño total de cada partición en cada lote
   (reportó 8.812.912 "filas escritas" sobre un lake de 6,1 M) → `contracts_written`
   cuenta los contratos aplicados (rama `fix/reporte-incremental`).
10. El aviso de cambio de año saltaba también con contratos ya conocidos en dos años →
    sólo avisa de cambios nuevos (rama `fix/reporte-incremental`).

Pendientes / limitaciones conocidas:

- Una fila con `ultima_actualizacion` nula que cambia otros campos sin cambiar de
  partición no es detectable (no hay señal); un `backfill --year Y --force` la recoge.
- Cada partición tocada se reescribe completa (contrato de un archivo por partición);
  `year=2025` pesa ~380 MB.
- Interpolación en `$where` de Socrata con f-strings: sólo valores internos (fechas,
  ids escapados), nunca entrada de usuario. `secop sql` ejecuta SQL arbitrario por
  diseño (herramienta local).
- `Settings` acepta el token heredado `ID_TOKEN` como alias de `SOCRATA_APP_TOKEN`.
- Bases sin columna de versión (interadministrativos): el estado guarda y la CLI
  muestra el watermark por defecto (2018-01-01), que ahí no significa nada.

## Otros archivos

- `memory-bank/`: contexto de producto, contrato de los datos, estrategia incremental,
  mapeo de columnas entre bases y estándares de código. Mantenerlos coherentes con el
  código al cambiar el diseño.
- `notebooks/exploracion.ipynb`: exploración con `QueryEngine` y `QuerySpec`
  (grupo de dependencias `analysis`).
- `data/` y `.env` están en `.gitignore`; no commitear datos ni secretos. `data/raw/`
  es la caché del ETL viejo; el lake local es `data/lake/`.
