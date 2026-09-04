# Dataset Contract

Dataset:

SECOP II

Primary Key

id_contrato

Incremental Column

ultima_actualizacion

Partition

year(fecha_de_inicio_del_contrato)

NULL dates or years outside [2000, current_year + 2] go to `year=unknown`.

Layout

`raw/contratos/year=YYYY/data.parquet` — exactly one compacted file per partition.

Storage

Parquet Snappy, in Cloudflare R2 (bucket `secop`).

Encoding

UTF-8

Timezone

UTC

Null Handling

Never replace NULL values automatically.

Dedup

On write, keep one row per `id_contrato`: the one with the greatest
`ultima_actualizacion`.
