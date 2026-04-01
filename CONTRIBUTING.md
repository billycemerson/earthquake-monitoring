# Runbook & Contributing Guide

## Setup

```bash
git clone https://github.com/billycemerson/earthquake-monitoring
cd earthquake-monitoring
make install
make pipeline
make status
```

## Skenario Operasional

### Pipeline normal
```bash
make pipeline
```

### Rebuild warehouse dari bronze (tanpa hit API)
```bash
rm -rf warehouse/
make backfill
make dbt-run && make dbt-test
```

### Backfill range tanggal
```bash
make backfill FROM=2026-03-01 TO=2026-03-31

# Dry run dulu
python ingestion/backfill.py --from 2026-03-01 --to 2026-03-31 --dry-run
```

### Investigasi anomali
```bash
make status

# Query DuckDB langsung
python -c "
import duckdb
con = duckdb.connect('warehouse/bmkg.duckdb')
rows = con.execute('''
    SELECT event_datetime, wilayah, magnitude, magnitude_category
    FROM fact_earthquake WHERE magnitude >= 6
    ORDER BY event_datetime DESC LIMIT 10
''').fetchall()
for r in rows: print(r)
"
```

### CI gagal karena freshness
```bash
# Cek BMKG API
curl -s https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json | python -m json.tool | head -20

# Trigger manual via gh CLI
gh workflow run ci.yml
```

## Override SLA Threshold

```bash
cd dbt_project/bmkg_pipeline
dbt run --vars '{"freshness_warn_hours": 6, "freshness_fail_hours": 24}'
```

## Menambahkan Test Baru

**dbt built-in** (schema.yml):
```yaml
- name: kolom
  tests:
    - not_null
    - accepted_values:
        values: ['A', 'B']
```

**Custom SQL** (tests/nama_test.sql):
```sql
-- Gagal kalau return rows
select event_id from {{ ref('stg_earthquake') }}
where kondisi_yang_salah = true
```

## Bronze Layer

Jangan hapus `data/bronze/` — satu-satunya sumber historis untuk backfill.
Untuk produksi jangka panjang, simpan di S3/GCS.
