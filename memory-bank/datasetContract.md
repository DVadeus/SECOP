# Dataset Contract

The lake holds two **bases** (`secop/bases.py`). Every ETL and query function takes a
`base`; the default is `contratos`, so the original behaviour is unchanged.

| | `contratos` | `interadministrativos` |
|---|---|---|
| Dataset | `jbjy-vk9h` (SECOP II contracts) | `s484-c9k3` (interadministrative agreements, SECOP I and II) |
| Size (2026-10) | ~6.1 M rows, 95 columns | ~87 k rows, 27 columns |
| Key | `id_contrato` (one row per contract) | `id_contrato` (**several rows per contract** in SECOP I: one per party / `origen_de_los_recursos`) |
| Version column | `ultima_actualizacion` (NULL in ~38 %) | none |
| Partition date | `fecha_de_inicio_del_contrato` | `fecha_inicio_del_contrato` (whole contract → earliest valid year) |
| Strategy | `snapshot_diff` (see `incrementalStrategy.md`) | `full_refresh` |
| File per partition | `data.parquet` | `interadministrativos.parquet` |
| State / index | `state/contratos.json`, `state/id_index.parquet` | `state/interadministrativos.json`, `state/interadministrativos_id_index.parquet` |
| Manifest / run marker | `_manifest.json`, `state/run_in_progress.json` | `_manifest_interadministrativos.json`, `state/interadministrativos_run_in_progress.json` |

Partition

year(partition date). NULL dates or years outside [2000, current_year + 2] go to
`year=unknown`.

Layout

`raw/contratos/year=YYYY/<data_file>` — both bases share the year folders; exactly one
compacted file **per base** per partition. Each reader globs only its own file
(`year=*/data.parquet`, `year=*/interadministrativos.parquet`), so views never mix and
legacy `part-*.parquet` are ignored.

`raw/contratos_historial/detectado=YYYY-MM-DD/<data_file>` — replaced versions per base,
with `year` (partition of origin), `historial_motivo`, `historial_detectado_utc`,
`historial_hash`. Append-only.

`backup/<timestamp>/` — repair backups.

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

Dedup — `contratos`

Within a partition, keep one row per `id_contrato`: the greatest
`ultima_actualizacion` wins; on a tie (or both NULL) the most recently downloaded wins.
Every losing version whose content differs is appended to the history.

Dedup — `interadministrativos` (grouped)

The unit of version is the whole contract (all its rows). Identical rows within a
contract are collapsed. If the contract's rows (multiset of content hashes) did not
change, it is kept as is and the partition is not rewritten; if any row changed, all
previous rows go to the history and the new rows replace them.

Across partitions (both bases)

The same `id_contrato` may legitimately appear in two real years (the source changed
its start date — kept as evidence). It must never appear both in `year=unknown` and in
a real year.

Column mapping between bases: `columnMapping.md`.
