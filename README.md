# 🌏 BMKG Earthquake Pipeline — v2

Production-ready batch pipeline untuk monitoring gempa bumi Indonesia dari BMKG.

**Upgrade dari v1:** Bronze layer · Incremental dbt models · Deduplication · Actionable observability · SLA enforcement · Unit tests

---

## Architecture

```
BMKG API (gempaterkini.json)
         │
         ▼
┌─────────────────┐
│  Python Ingestor │  fetch_bmkg.py
└─────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Bronze Layer  (data/bronze/)       │  raw JSON snapshot, partisi per hari
│  ingestion_date=YYYY-MM-DD/         │
│  earthquakes.json                   │
└─────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  DuckDB          │  warehouse/bmkg.duckdb
│  raw_earthquake  │
└─────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  dbt                                │
│  staging/stg_earthquake             │  incremental + dedup
│  marts/fact_earthquake              │  incremental
│  monitoring/earthquake_quality_summary │  OK/WARNING/FAILED
└─────────────────────────────────────┘
         │
         ▼
┌──────────────────────┐
│  CI + SLA Gate        │  GitHub Actions, run setiap jam
│  unit-tests → pipeline│
└──────────────────────┘
```

---

## Quick Start

```bash
make install     # Install semua dependencies
make pipeline    # Full pipeline: fetch + dbt run + dbt test
make status      # Lihat monitoring summary
```

---

## SLA Definition

| Metric            | OK       | WARNING  | FAILED   |
|-------------------|----------|----------|----------|
| Data freshness    | < 2 jam  | 2–12 jam | > 12 jam |
| Major earthquake  | Tidak ada| Ada (M≥7)| —        |
| Invalid magnitude | 0 records| Ada      | —        |

CI **fail** jika freshness > 12 jam. WARNING tidak menghentikan pipeline tapi terekam.

---

## Data Layers

### Bronze — Raw Snapshot
```
data/bronze/
└── ingestion_date=2026-04-01/
    └── earthquakes.json
```
Setiap file: `ingestion_time`, `source_url`, `record_count`, `records[]`

**Kenapa file dulu?** Bisa replay, debug, dan backfill tanpa hit API ulang.

### Staging (`stg_earthquake`)
Incremental · Deduplication via `ROW_NUMBER()` · Parse lat/lon · Kategorisasi magnitude/depth · Validasi flag

### Fact (`fact_earthquake`)
Incremental append · `event_date`, `event_hour`, `is_major_earthquake`

### Monitoring (`earthquake_quality_summary`)
`pipeline_status`: OK / WARNING / FAILED · Sub-status per dimensi

---

## Design Decisions

**Kenapa DuckDB?**
Data BMKG berskala kecil-menengah. DuckDB memberikan OLAP performance tanpa infrastruktur. Path upgrade: DuckDB → MotherDuck atau Iceberg on S3.

**Kenapa batch, bukan streaming?**
BMKG API adalah endpoint polling, bukan event stream. Batch per jam memenuhi SLA freshness < 2 jam.

**Kenapa bronze layer?**
Tanpa bronze, kalau ada bug di transform, data hilang dan tidak bisa di-recover (BMKG hanya expose N gempa terbaru). Bronze adalah immutable source of truth.

**Kenapa SHA256 bukan MD5?**
MD5 collision-prone untuk production. SHA256 (truncated 16 char) lebih aman sebagai stable surrogate key.

---

## Failure Scenarios

| Skenario | Behavior |
|---|---|
| BMKG API down | exit(1) dengan log error, CI fail, bronze tidak dibuat |
| Partial API response | Bronze disimpan as-is, dbt test catch anomali |
| Duplicate event | `INSERT OR REPLACE` + `ROW_NUMBER` dedup — idempotent |
| Pipeline jalan 2x | Bronze di-overwrite, DuckDB replace, dbt skip jika tidak ada data baru |
| Gempa besar (M≥7) | `pipeline_status = WARNING`, CI tidak fail tapi visible |
| Freshness > 12 jam | `pipeline_status = FAILED`, CI exit(1) |

---

## Scaling Strategy

**10x data** — Tidak perlu ubah apapun. DuckDB + incremental model handle ini.

**100x data** — Ganti bronze format JSON → Parquet (10x lebih kecil).

**1000x / multi-source** — Bronze ke S3/GCS, DuckDB via httpfs, atau migrasi ke MotherDuck. Orchestration: Airflow/Dagster.

---

## Project Structure

```
.
├── data/bronze/                         # Raw snapshots (auditable)
├── ingestion/
│   ├── fetch_bmkg.py                   # Ingest: API → bronze → DuckDB
│   └── backfill.py                     # Replay bronze ke DuckDB
├── dbt_project/bmkg_pipeline/
│   ├── models/staging/                 # Incremental + dedup
│   ├── models/marts/                   # Fact table
│   ├── models/monitoring/              # Observability
│   ├── macros/                         # Reusable SQL logic
│   ├── tests/                          # Custom SQL tests
│   └── packages.yml                    # dbt-utils
├── tests/
│   └── test_fetch_bmkg.py             # 10 unit tests Python
├── .github/workflows/ci.yml           # 2-job CI + SLA gate
├── requirements.txt
├── Makefile
└── CONTRIBUTING.md
```
