# Dataset Contract

Dataset:

SECOP II

Primary Key

id_contrato

Version Column

ultima_actualizacion (NULL in ~38 % of the source; not used as a global watermark —
see `incrementalStrategy.md`)

Partition

year(fecha_de_inicio_del_contrato)

NULL dates or years outside [2000, current_year + 2] go to `year=unknown`.

Layout

`raw/contratos/year=YYYY/data.parquet` — exactly one compacted file per partition.
Readers glob only `year=*/data.parquet` (legacy `part-*.parquet` are ignored).

`raw/contratos_historial/detectado=YYYY-MM-DD/data.parquet` — replaced versions, with
`year` (partition of origin), `historial_motivo`, `historial_detectado_utc`,
`historial_hash`. Append-only.

`state/contratos.json` (state), `state/id_index.parquet` (id × partition index),
`_manifest.json` (counts of the whole lake), `backup/<timestamp>/` (repair backups).

Storage

Parquet Snappy, in Cloudflare R2 (bucket `secop`).

Encoding

UTF-8

Timezone

UTC

Null Handling

Never replace NULL values automatically.

Deletion

Never delete a contract from the lake, even if the source no longer publishes it.

Dedup

Within a partition, keep one row per `id_contrato`: the greatest
`ultima_actualizacion` wins; on a tie (or both NULL) the most recently downloaded wins.
Every losing version whose content differs is appended to the history.

Across partitions: the same `id_contrato` may legitimately appear in two real years
(the source changed its start date — kept as evidence). It must never appear both in
`year=unknown` and in a real year.
