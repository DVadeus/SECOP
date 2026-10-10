# SECOP II Data Lake

ETL incremental de los contratos de SECOP II publicados en
[datos.gov.co](https://www.datos.gov.co/resource/jbjy-vk9h.json) hacia un data lake
de Parquet en **Cloudflare R2**, más una CLI para consultar ese lake con **SQL de
DuckDB** y exportar a Excel.

El objetivo es reemplazar la búsqueda manual en SECOP / PACO contratista por consultas
SQL reproducibles para la UAN del Concejo de Bogotá.

```
datos.gov.co (API Socrata)
        │  secop etl {backfill|incremental}
        ▼
Cloudflare R2 :  raw/contratos/year=YYYY/data.parquet   (uno por año, compactado)
                 state/contratos.json                    (watermark incremental)
        │  secop query / secop sql   (DuckDB + httpfs)
        ▼
pandas DataFrame ──► Excel / CSV / Parquet
```

## Componentes

| Módulo | Rol |
|---|---|
| `secop/extract.py` | Cliente Socrata: paginación estable (`$order=:id`) y reintentos. |
| `secop/transform.py` + `secop/schema.py` | JSON crudo → DataFrame tipado (fechas, numéricos, booleanos `Si/No`, categóricas). Tolerante a *schema drift*. |
| `secop/storage/` | Abstracción de objetos: `R2Store` (boto3) y `LocalStore`. |
| `secop/lake.py` | Escritura Parquet **compactada**: `drop_duplicates(id_contrato, keep=última ultima_actualizacion)`. Un `data.parquet` por partición. |
| `secop/state.py` | Watermark (`ultima_actualizacion`) en el object store. |
| `secop/pipeline.py` | `backfill()` (histórico, reanudable, mes a mes) e `incremental()` (upsert por partición). |
| `secop/query/` | Motor DuckDB sobre el lake, constructor de filtros parametrizados, exportador. |
| `secop/cli.py` | App Typer: `etl`, `query`, `sql`, `info`. |

## Requisitos

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/)

```bash
uv sync
```

## Configuración

Copia `.env.example` a `.env` y rellena:

```
SOCRATA_APP_TOKEN=...        # datos.gov.co -> Developer settings
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=secop
```

### Crear el bucket y el token en Cloudflare R2

1. Dashboard de Cloudflare → **R2** → *Create bucket* → nombre `secop`.
2. R2 → *Manage R2 API Tokens* → *Create API token* → permiso **Object Read & Write**,
   *Specify bucket* = `secop`.
3. Copia **Access Key ID**, **Secret Access Key** y el **Account ID** al `.env`.
4. Para la tarea programada, agrega los mismos valores como *repository secrets* en
   GitHub (`Settings → Secrets and variables → Actions`): `SOCRATA_APP_TOKEN`,
   `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`.

## Uso

### Carga histórica (una vez, en local)

Descarga fresca desde datos.gov.co, año por año. Es reanudable: si un año ya está en
R2 se omite (usa `--force` para rehacerlo).

```bash
uv run secop etl backfill --start 2018 --end 2026 --target r2
```

### Incremental (lo ejecuta GitHub Actions cada semana)

```bash
uv run secop etl incremental --lookback-days 2
```

### Consultar por características

```bash
# Contratos de una entidad contratante (NIT) desde 2024
uv run secop query --nit-entidad 900475780 --year-from 2024 -o unp.xlsx

# Una empresa como contratista, varias a la vez
uv run secop query --nit-proveedor 830125075 --nit-proveedor 900581703 -o proveedores.xlsx

# Búsqueda por texto + filtros estructurados + proyección de columnas
uv run secop query \
  --entidad-contains "METRO DE BOGOTA" \
  --estado "Activo" --valor-min 100000000 \
  --columns id_contrato,proveedor_adjudicado,valor_del_contrato \
  -o metro.csv
```

Filtros disponibles: `--nit-proveedor`, `--nit-entidad` (repetibles),
`--entidad-contains`, `--proveedor-contains`, `--departamento`, `--estado`,
`--modalidad`, `--year-from/--year-to`, `--valor-min/--valor-max`, `--columns`,
`--limit`. El formato de salida lo decide la extensión (`.xlsx` / `.csv` / `.parquet`);
si un `.xlsx` supera el límite de filas de Excel se escribe CSV automáticamente.

### SQL directo

```bash
uv run secop sql "SELECT year, count(*) FROM contratos GROUP BY 1 ORDER BY 1"
uv run secop sql "SELECT * FROM contratos WHERE documento_proveedor = '901261110'" -o foks.xlsx
```

### Historial de versiones

Cuando un contrato cambia (llega una versión con otro contenido), el lake principal
guarda la versión vigente y la anterior se conserva en
`raw/contratos_historial/detectado=AAAA-MM-DD/data.parquet` con el motivo y la fecha de
detección. Nunca se borra. Se consulta aparte, para que `secop query` no devuelva
duplicados:

```bash
uv run secop historial --nit-proveedor 830125075 -o historial_proveedor.xlsx
uv run secop historial --nit-entidad 900475780 -o historial_unp.xlsx
```

En `secop sql` la vista se llama `contratos_historial`.

### Estado del lake

```bash
uv run secop info
```

### Sin conexión (Parquet local)

Todos los comandos de consulta aceptan `--source local [--path <dir>]`. Sin `--path`
se lee el lake local por defecto, `data/lake/raw/contratos/` (configurable con
`SECOP_LOCAL_DATA_DIR`), que se llena con `secop etl ... --target local`:

```bash
uv run secop query --nit-entidad 900475780 --source local -o unp.xlsx
```

Sólo se leen los `year=*/data.parquet` compactados. `data/raw/` es la caché del ETL
viejo (archivos `part-*.parquet` sin deduplicar) y no debe usarse como lake.

### Notebook

`notebooks/exploracion.ipynb` usa el mismo motor (`secop.query`) para explorar de forma
interactiva y exportar a Excel.

## Desarrollo

```bash
uv run ruff check . && uv run ruff format --check .
uv run basedpyright secop
uv run pytest                 # las pruebas que tocan Socrata/R2 real están marcadas 'integration' y excluidas
```

## Contrato del dataset

- Clave primaria: `id_contrato`
- Columna incremental: `ultima_actualizacion`
- Partición: `year(fecha_de_inicio_del_contrato)`; fechas nulas o años fuera de
  `[2000, año+2]` → `year=unknown`
- Almacenamiento: Parquet + Snappy, UTF-8, UTC
- Los NULL nunca se rellenan automáticamente

Detalle en `memory-bank/`.
