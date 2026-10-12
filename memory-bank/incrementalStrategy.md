# Incremental Strategy

The ETL is incremental, but **not watermark-driven**. Each base has its strategy
(`Base.strategy` in `secop/bases.py`):

- `contratos` (`snapshot_diff`): light snapshot + id index, described below.
- `interadministrativos` (`full_refresh`): ~87 k rows and no version column, so every
  run downloads the whole dataset (`extract.fetch_all_checked`, `rowsUpdatedAt`
  checked before and after) and merges it by content: the multiset of row hashes of
  each contract decides whether it changed. Unchanged partitions are not rewritten; a
  second run with no source changes writes nothing. `backfill` for this base ignores
  years and does the same full refresh.

The rules below (never delete, cross-partition rules, restart, repair) apply to both
bases; in `interadministrativos` they act on the whole contract (all its rows).

---

## Why not a watermark

Measured on 2026-10-09:

- `ultima_actualizacion` is NULL in ~38 % of the source (2.1 M of 5.6 M rows), and new
  contracts usually arrive without it. A `WHERE ultima_actualizacion > X` query never
  sees them (95,467 contracts were missing from R2 because of this).
- datos.gov.co reloads the whole dataset daily: Socrata's `:created_at`/`:updated_at`
  are the same for every row and `:id` changes, so system fields are useless too.

---

## Light snapshot + id index

1. Download a **light snapshot** of the whole source: only `id_contrato`,
   `ultima_actualizacion`, `fecha_de_inicio_del_contrato` (~10 min).
   `rowsUpdatedAt` (metadata API) is checked before and after; if the source was
   reloaded in between, the run aborts without writing (`SourceReloadedError`).
2. Compare it with the **id index** `state/id_index.parquet` (one row per
   `id_contrato` × partition, mirrors the lake) — `secop.diff`. Fetch an id when:
   - it is not in its target partition (new contract, `unknown` that now has a date,
     start-year change), or
   - it is there but the source has a newer `ultima_actualizacion`
     (or the lake has it NULL), or
   - the source dropped its start date and brings a newer version.
3. Download only those ids (`id_contrato in (...)`, 200 per request) and apply them in
   batches of 50,000 ids.

`ultima_actualizacion` in `state/contratos.json` is informational only (max in the
index). A resumed backfill can no longer cause lost updates: each id is compared with
its own version.

Known blind spot: a row with NULL `ultima_actualizacion` that changes other fields
without changing partition is not detected (there is no signal). A forced backfill of
that year picks it up (it merges, never deletes).

---

## Never delete

Contracts are never removed from the lake, even if the source stops publishing them
(as of 2026-10-09, 491,931 ids in R2 are no longer in the source, mostly `Borrador` /
`Cancelado`). SECOP is a national public record; disappearances are evidence.

- Backfill always merges with existing partitions, also with `--force`.
- Replaced versions go to `raw/contratos_historial/detectado=YYYY-MM-DD/data.parquet`
  (see `datasetContract.md`), queried with `secop historial`.

---

## Cross-partition rules

- `year=unknown` → real year: the dated version is written to its year; the undated copy
  leaves `unknown` and goes to history (`fecha_completada`).
- Year A → year B: **both copies are kept** in the main lake (evidence of the source
  changing the start date); the run logs a warning.
- Real year → no date in source: the dated version stays; the new undated version goes
  to history (`fecha_eliminada_en_origen`).

---

## Restart

Each run writes `state/run_in_progress.json` first and deletes it last. If it exists
when a run starts, the previous run crashed after writing some partitions but before
saving the index: the index is rebuilt from the lake (~30 s on R2). History is written
before the partition it comes from, and appends are idempotent (content hash), so a
re-run never loses or duplicates versions. Backfill is resumable per year.

---

## Repair

`secop etl reparar [--aplicar]` fixes data written by older ETL versions: moves out of
`year=unknown` the contracts that already exist in a real year (to history), after
backing up `year=unknown`, state and manifest under `backup/<timestamp>/`.
