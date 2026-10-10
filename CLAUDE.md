# CLAUDE.md

Guía para trabajar en este repositorio y registro del análisis del código.
Última revisión: 2026-10-10 (rama `fix/integridad-lake`).

## Qué es

ETL de los contratos de **SECOP II** (datos.gov.co, dataset Socrata `jbjy-vk9h`) hacia
un data lake Parquet en **Cloudflare R2**, más una CLI (`secop`) que consulta el lake con
**DuckDB** y exporta a Excel/CSV/Parquet. Usuario final: la UAN del Concejo de Bogotá
(reemplaza búsquedas manuales en SECOP / PACO).

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

uv run secop etl backfill --start 2018 --end 2026 --target r2 [--year Y --force]
uv run secop etl incremental [--target local]
uv run secop etl reparar [--aplicar]      # sin --aplicar sólo informa
uv run secop query --nit-entidad 900475780 --year-from 2024 -o out.xlsx [--source local --path DIR]
uv run secop historial --nit-proveedor 830125075 -o versiones.xlsx
uv run secop sql "SELECT year, count(*) FROM contratos GROUP BY 1" [-o out.csv]
uv run secop info [--source local]
```

Estado: 78 tests pasan, ruff y basedpyright sin errores.

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
| `secop/config.py` | Constantes (dataset, PK, prefijos, claves de estado) y `Settings` (pydantic-settings, `.env` en la raíz; `get_settings()` cacheado). |
| `secop/extract.py` | `iter_pages`/`fetch_data` (`$order=:id`, 4 reintentos, `$select` opcional). `fetch_snapshot` (foto ligera, verifica `rowsUpdatedAt` antes y después → `SourceReloadedError`). `fetch_by_ids` (`id_contrato in (...)`, 200 por petición). |
| `secop/schema.py` / `transform.py` | Clasificación de columnas y tipado de registros crudos. Nunca rellena NULLs. |
| `secop/lake.py` | Partición `year=YYYY` por `fecha_de_inicio_del_contrato` (nulo o fuera de `[2000, año+2]` → `year=unknown`). `merge_versions`: una versión por id (gana `ultima_actualizacion` mayor; empate → la recién llegada) y devuelve las perdedoras con contenido distinto. `upsert_partition` escribe historial **antes** que la partición y nunca elimina ids. `move_out_ids` saca ids de una partición pasándolos al historial. |
| `secop/history.py` | Historial anexable en `raw/contratos_historial/detectado=YYYY-MM-DD/`; columnas `year`, `historial_motivo` (`version_reemplazada`, `fecha_completada`, `fecha_eliminada_en_origen`), `historial_detectado_utc`, `historial_hash`. Idempotente por hash de contenido (`None`≡`NaN`). |
| `secop/index.py` | Índice `state/id_index.parquet`: una fila por (id, partición) con su `ultima_actualizacion`. Reconstruible desde el lake (`build_from_lake`, ~30 s en R2). Da `row_counts` y el watermark informativo. |
| `secop/diff.py` | DuckDB: qué ids bajar comparando la foto con el índice (no está en su partición destino; versión más nueva; origen borró la fecha y trae versión nueva). |
| `secop/pipeline.py` | `_apply_batch` aplica las reglas entre particiones. `backfill()` (por año, reanudable, siempre fusiona), `incremental()` (foto → diff → fetch por lotes de 50.000 ids), `repair()` (saca de `unknown` los ids que ya están con fecha, con respaldo). `_begin`/`_finish` gestionan la marca `state/run_in_progress.json`, índice, estado y manifest. |
| `secop/state.py` | `PipelineState` en `state/contratos.json`. `ultima_actualizacion` es **informativo** (máximo del índice); la extracción no depende de él. |
| `secop/storage/` | Protocolo `ObjectStore` y `R2Store` (boto3) / `LocalStore` (escritura atómica). |
| `secop/query/` | `QueryEngine` con vista `contratos` (glob `year=*/data.parquet`) y, si existe, `contratos_historial`. `filters.build_query` / `build_history_query` (todo parametrizado). Exportador por extensión. |
| `secop/cli.py` | Typer: `etl backfill|incremental|reparar`, `query`, `historial`, `sql`, `info`. |

Reglas entre particiones (`pipeline._apply_batch`):

- `year=unknown` → año real: la versión con fecha va a su año; la copia sin fecha sale
  de `unknown` al historial (`fecha_completada`).
- Año A → año B: se conservan ambas copias en el lake (evidencia); se avisa en el log.
- Año real → sin fecha en el origen: se conserva la versión con fecha; la nueva va al
  historial (`fecha_eliminada_en_origen`).

CI: `.github/workflows/etl-weekly.yml` corre `etl incremental` los lunes 06:00 UTC
(un reintento a los 15 min si falla, p.ej. por recarga del origen; timeout 120 min).
Con `workflow_dispatch` + `year` hace `backfill --year Y --force` (fusiona, no borra).

## Convenciones

De `memory-bank/codingStandards.md` y del propio código:

- Python ≥ 3.13, `from __future__ import annotations`, type hints en todo.
- `pathlib`, `logging` (nunca `print` fuera de la salida Rich de la CLI).
- Funciones de < 50 líneas; dataclasses (`slots=True`) para estructuras de datos.
- No capturar `Exception` genérica salvo para relanzar (única excepción: `_print_state`
  en la CLI, marcada `noqa: BLE001`).
- Docstrings y mensajes en español. Ruff (`E,F,I,UP,B`, línea 88, comillas dobles).
- Tests con `LocalStore` en `tmp_path`, origen simulado con `monkeypatch` sobre
  `pipeline.fetch_snapshot`/`fetch_by_ids`; nada de red. Tests contra Socrata/R2 reales
  llevan `@pytest.mark.integration`.
- Constantes de dataset en `config.py`; no hardcodear nombres de columna.
- En Git Bash de Windows, los heredocs con `\\n` se convierten en saltos reales: para
  editar código con secuencias de escape usar la herramienta Edit, no scripts.

## Contrato de datos (no romper)

Detalle en `memory-bank/datasetContract.md` e `incrementalStrategy.md`.

- Nunca borrar contratos del lake. Toda versión reemplazada va al historial.
- Exactamente un `data.parquet` compactado por partición; nunca `part-*`.
- Un id nunca debe estar a la vez en `year=unknown` y en un año real.
- Historial antes que partición; índice, estado y manifest al final; marca de corrida.
- Nunca rellenar NULLs. Tiempos tratados como UTC (naive en Parquet).
- `year` es string (partición Hive); usar `TRY_CAST(year AS INTEGER)`.
- Si se cambian tipos o columnas en `schema.py`, subir `SCHEMA_VERSION`.

## Hallazgos del análisis y estado

Auditoría de R2 vs origen (2026-10-09/10, sólo lectura):

| Hallazgo | Medida | Estado |
|---|---|---|
| Origen se recarga a diario; `:updated_at`/`:id` inútiles | todas las filas con el mismo `:updated_at` | Diseño: foto + índice |
| `ultima_actualizacion` nula en el origen | 2.139.277 de 5.621.358 (38 %) | Etapa 5–6 |
| Contratos del origen ausentes en R2 (nuevos sin `ua`) | 95.467 | Etapa 5; se cargan en el próximo incremental |
| `unknown` en R2 que ya tienen fecha en el origen | 30.869 | Etapa 5; próximo incremental |
| Ids en `unknown` y además en un año real | 1.916 | Etapa 3 + `etl reparar` |
| Ids en dos años reales | 10 | Se conservan (evidencia) |
| Ids en R2 que ya no están en el origen | 491.931 (sobre todo Borrador/Cancelado) | Se conservan (decisión) |
| Ids duplicados dentro del propio origen | 298 | `merge_versions` |

Defectos del código y etapa que los corrigió (commits en `fix/integridad-lake`):

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

Pendientes / limitaciones conocidas:

- Una fila con `ultima_actualizacion` nula que cambia otros campos sin cambiar de
  partición no es detectable (no hay señal); un `backfill --year Y --force` la recoge.
- Cada partición tocada se reescribe completa (contrato de un archivo por partición);
  `year=2025` pesa ~380 MB.
- Interpolación en `$where` de Socrata con f-strings: sólo valores internos (fechas,
  ids escapados), nunca entrada de usuario. `secop sql` ejecuta SQL arbitrario por
  diseño (herramienta local).
- `Settings` acepta el token heredado `ID_TOKEN` como alias de `SOCRATA_APP_TOKEN`.

## Otros archivos

- `memory-bank/`: contexto de producto, contrato de dataset, estrategia incremental y
  estándares de código. Mantenerlos coherentes con el código al cambiar el diseño.
- `notebooks/exploracion.ipynb`: exploración con `QueryEngine` y `QuerySpec`
  (grupo de dependencias `analysis`).
- `data/` y `.env` están en `.gitignore`; no commitear datos ni secretos. `data/raw/`
  es la caché del ETL viejo; el lake local es `data/lake/`.
