# Product Context

## Project

SECOP Data Lake ETL

## Purpose

Two responsibilities:

1. A production-grade **ETL** that incrementally extracts SECOP II procurement data
   from datos.gov.co and maintains a reliable, reproducible Parquet Data Lake in
   **Cloudflare R2**.
2. A **query layer** (CLI `secop` + notebook) that reads that lake with DuckDB SQL,
   materializes a pandas DataFrame and exports to Excel/CSV for the UAN of the
   Concejo de Bogotá.

Machine Learning, dashboards and statistical analysis remain out of scope; the query
layer only filters/aggregates and exports.

---

## Dataset

Source:

https://www.datos.gov.co/resource/jbjy-vk9h.json

Dataset:

SECOP II contracts

---

## Objectives

The ETL must:

- download new records
- avoid duplicate downloads
- detect updated records
- preserve every replaced version (`raw/contratos_historial/`, `secop historial`)
- never delete a contract, even if the source removes it (it may be evidence)
- generate parquet files (one compacted `data.parquet` per `year=` partition)
- upload them to Cloudflare R2 (S3 API)
- maintain metadata (`state/contratos.json`, `state/id_index.parquet`, `_manifest.json`)
- be restartable after failures
- be idempotent (upsert by `id_contrato`)
- minimize API usage
- run weekly on GitHub Actions (incremental); full backfill runs locally, once

---

## Non Objectives

The project MUST NOT:

- create dashboards
- perform statistical analysis
- execute machine learning
- calculate KPIs / generate narrative reports

The query layer only filters, aggregates and exports tabular data on demand.

---

## Principles

The ETL must prioritize:

Correctness > Performance

Reliability > Speed

Reproducibility > Convenience

Every execution should be deterministic.

---

## Expected lifetime

Several years.

The architecture should support future datasets from datos.gov.co without major modifications.
