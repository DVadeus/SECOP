# Incremental Strategy

The ETL is incremental.

The extraction never scans the full dataset after the initial load.

---

## Watermark

The ETL stores `ultima_actualizacion` in `state/contratos.json` **in the object store**
(R2), not on local disk, after every successful execution. This keeps the GitHub
Actions runner stateless.

A `lookback_days` window (default 2) is subtracted from the watermark on each run to
absorb late-arriving updates; the upsert makes the overlap harmless.

---

## Query

```
WHERE ultima_actualizacion > last_watermark
```

---

## Duplicate detection

Primary Key: `id_contrato`

Version: `ultima_actualizacion` (greatest wins)

Applied at write time in `secop.lake.compact()` — both within the incoming batch and
against the existing partition file.

---

## Restart

If the ETL crashes, the watermark is NOT updated (it is written last, only after every
touched partition uploaded successfully). The next execution re-fetches the overlap and
the upsert deduplicates it. Backfill is resumable per year: an existing
`year=YYYY/data.parquet` is skipped unless `--force`.
