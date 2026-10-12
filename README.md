# SECOP II Data Lake

ETL de dos bases de datos.gov.co hacia un data lake de Parquet en **Cloudflare R2**,
más una CLI para consultar ese lake con **SQL de DuckDB** y exportar a Excel:

| Base (`--base`) | Dataset | Contenido |
|---|---|---|
| `contratos` (por defecto) | [`jbjy-vk9h`](https://www.datos.gov.co/resource/jbjy-vk9h.json) | Contratos SECOP II (~6,1 M, 95 columnas) |
| `interadministrativos` | [`s484-c9k3`](https://www.datos.gov.co/resource/s484-c9k3.json) | Convenios interadministrativos SECOP I y II (~87 mil filas, 27 columnas) |

El objetivo es reemplazar la búsqueda manual en SECOP / PACO contratista por consultas
SQL reproducibles para la UAN del Concejo de Bogotá.

```
datos.gov.co (API Socrata)
        │  secop etl {backfill|incremental} [--base ...]
        ▼
Cloudflare R2 :  raw/contratos/year=YYYY/data.parquet                  (contratos)
                 raw/contratos/year=YYYY/interadministrativos.parquet  (interadministrativos)
                 state/<base>.json, índice y manifest por base
        │  secop query / secop sql   (DuckDB + httpfs; una vista por base)
        ▼
pandas DataFrame ──► Excel / CSV / Parquet
```

## Componentes

| Módulo | Rol |
|---|---|
| `secop/bases.py` | Definición de cada base: dataset, archivo por partición, claves de estado, esquema, estrategia y mapeo de columnas. |
| `secop/extract.py` | Cliente Socrata: paginación estable (`$order=:id`) y reintentos. |
| `secop/transform.py` + `secop/schema.py` | JSON crudo → DataFrame tipado (fechas, numéricos, booleanos `Si/No`, categóricas). Tolerante a *schema drift*. |
| `secop/storage/` | Abstracción de objetos: `R2Store` (boto3) y `LocalStore`. |
| `secop/lake.py` | Escritura Parquet **compactada**: `drop_duplicates(id_contrato, keep=última ultima_actualizacion)`. Un `data.parquet` por partición. |
| `secop/state.py` | Watermark (`ultima_actualizacion`) en el object store. |
| `secop/pipeline.py` | `backfill()` (histórico, reanudable, mes a mes) e `incremental()` (upsert por partición). |
| `secop/query/` | Motor DuckDB sobre el lake, constructor de filtros parametrizados, exportador. |
| `secop/cli.py` | App Typer: `etl`, `query`, `historial`, `sql`, `info` (con `--base`). |

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

Descarga desde datos.gov.co, año por año. Es reanudable: si un año ya está en R2 se
omite (usa `--force` para volver a descargarlo). Siempre **fusiona** con lo que ya hay:
nunca elimina contratos, aunque el origen ya no los publique.

```bash
uv run secop etl backfill --start 2018 --end 2026 --target r2
```

### Incremental (lo ejecuta GitHub Actions cada semana)

```bash
uv run secop etl incremental
```

`ultima_actualizacion` es nula en ~38 % del origen y datos.gov.co recarga el dataset
completo a diario, así que no sirve como watermark. El incremental baja una foto ligera
del origen (`id_contrato`, `ultima_actualizacion`, `fecha_de_inicio_del_contrato`,
~10 min), la compara con el índice de ids del lake (`state/id_index.parquet`) y
descarga completos sólo los contratos nuevos, con versión más nueva, o que pasaron de
`year=unknown` a un año real. Si el origen se recarga durante la foto, la corrida
aborta sin escribir y se reintenta.

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

### Convenios interadministrativos (`--base interadministrativos`)

Todos los comandos de ETL y de consulta aceptan `--base`. Sin ella se usa
`contratos`, así que ningún uso anterior cambia.

```bash
# Convenios de SECOP I de más de mil millones
uv run secop query --base interadministrativos --plataforma SECOP_I --valor-min 1e9 -o conv.xlsx

# Un contratista en los convenios (identificacion_contratista)
uv run secop query --base interadministrativos --nit-proveedor 899999027 -o conv_dane.xlsx

uv run secop info --base interadministrativos
uv run secop etl incremental --base interadministrativos
```

Los filtros se escriben igual que en contratos y se traducen a las columnas de esta
base: `--nit-proveedor` → `identificacion_contratista`, `--proveedor-contains` →
`contratista`, `--modalidad` → `modalidad_contratacion`, `--valor-min/max` →
`valor_con_adiciones`. Además:

- `--plataforma SECOP_I|SECOP_II` filtra por la columna `fuente`.
- **`--nit-entidad`**: esta base no tiene NIT de entidad. Se resuelve con el
  `codigo_entidad` de la base de contratos (`id_entidad` coincide al 100 %), así que
  solo encuentra convenios de **SECOP II**. Para SECOP I usa `--entidad-contains`.

Particularidades de la base:

- **Un contrato puede tener varias filas** (en SECOP I, una por parte o por
  `origen_de_los_recursos`). La versión es el contrato completo: si cambia una
  fila, todas sus filas anteriores pasan al historial. Para contar contratos usa
  `count(DISTINCT id_contrato)`.
- No tiene `ultima_actualizacion`: cada semana se baja completa (~1 min) y el hash del
  contenido decide qué cambió. Nunca se borra nada.
- El 95 % de sus convenios de SECOP II también está en `contratos`, con más columnas.

### SQL directo

Vistas disponibles: `contratos`, `interadministrativos`, `contratos_historial` e
`interadministrativos_historial`.

```bash
uv run secop sql "SELECT year, count(*) FROM contratos GROUP BY 1 ORDER BY 1"
uv run secop sql "SELECT * FROM contratos WHERE documento_proveedor = '901261110'" -o foks.xlsx
# Convenios de SECOP II que están en ambas bases
uv run secop sql "SELECT c.id_contrato, c.nit_entidad, i.fecha_cargue FROM contratos c JOIN interadministrativos i USING (id_contrato)"
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

Con `--base interadministrativos` se consulta el historial de los convenios. En
`secop sql` las vistas se llaman `contratos_historial` e
`interadministrativos_historial`.

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

Cada vista lee solo el archivo de su base en cada `year=*` (`data.parquet` o
`interadministrativos.parquet`). `data/raw/` es la caché del ETL viejo (archivos
`part-*.parquet` sin deduplicar) y no debe usarse como lake.

### Notebook

`notebooks/exploracion.ipynb` usa el mismo motor (`secop.query`) para explorar de forma
interactiva y exportar a Excel.

## Desarrollo

```bash
uv run ruff check . && uv run ruff format --check .
uv run basedpyright secop
uv run pytest                 # las pruebas que tocan Socrata/R2 real están marcadas 'integration' y excluidas
```

## Contrato de los datos

- Identificador: `id_contrato` en ambas bases.
- Versión: `ultima_actualizacion` en contratos. Interadministrativos no tiene
  columna de versión: se compara el contenido del contrato completo.
- Partición: año de la fecha de inicio (`fecha_de_inicio_del_contrato` /
  `fecha_inicio_del_contrato`); fechas nulas o años fuera de `[2000, año+2]` →
  `year=unknown`. En interadministrativos, un contrato con varias filas va completo
  al año más temprano.
- Un archivo por base y partición; nunca se borra un contrato.
- Almacenamiento: Parquet + Snappy, UTF-8, UTC
- Los NULL nunca se rellenan automáticamente

Detalle en `memory-bank/`.
