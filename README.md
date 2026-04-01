# BMKG Earthquake Pipeline

Batch pipeline untuk monitoring gempa bumi Indonesia dari BMKG.

## Quick Start

```bash
pip install duckdb requests dbt-core dbt-duckdb
make pipeline
```

## Architecture

```
BMKG API → DuckDB (raw) → dbt (staging → fact → monitoring) → CI
```
