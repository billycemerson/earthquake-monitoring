.PHONY: help install fetch backfill dbt-deps dbt-run dbt-test dbt-docs pipeline test bronze-ls status clean

PROFILES_DIR = ../profiles
DBT_DIR      = dbt_project/bmkg_pipeline
TARGET       ?= dev

help:
	@printf "\n  BMKG Earthquake Pipeline v2\n"
	@printf "  ─────────────────────────────────────────\n"
	@printf "  make install             Install semua dependencies\n"
	@printf "  make fetch               Ingest BMKG API → bronze → DuckDB\n"
	@printf "  make backfill            Replay semua bronze ke DuckDB\n"
	@printf "  make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD\n"
	@printf "  make dbt-run             Jalankan semua dbt models\n"
	@printf "  make dbt-test            Jalankan dbt tests\n"
	@printf "  make dbt-docs            Generate & serve dbt docs\n"
	@printf "  make pipeline            Full: fetch + dbt-run + dbt-test\n"
	@printf "  make test                Unit tests (pytest)\n"
	@printf "  make bronze-ls           List semua bronze snapshots\n"
	@printf "  make status              Lihat monitoring summary\n"
	@printf "  make clean               Hapus warehouse & artifacts\n\n"

install:
	pip install -r requirements.txt pytest pytest-mock

fetch:
	python ingestion/fetch_bmkg.py

backfill:
	@if [ -n "$(FROM)" ] && [ -n "$(TO)" ]; then \
		python ingestion/backfill.py --from $(FROM) --to $(TO); \
	else \
		python ingestion/backfill.py; \
	fi

dbt-deps:
	cd $(DBT_DIR) && dbt deps --profiles-dir $(PROFILES_DIR)

dbt-run: dbt-deps
	cd $(DBT_DIR) && dbt run --profiles-dir $(PROFILES_DIR) --target $(TARGET)

dbt-test: dbt-deps
	cd $(DBT_DIR) && dbt test --profiles-dir $(PROFILES_DIR) --target $(TARGET)

dbt-docs: dbt-deps
	cd $(DBT_DIR) && dbt docs generate --profiles-dir $(PROFILES_DIR)
	cd $(DBT_DIR) && dbt docs serve --port 8080 --profiles-dir $(PROFILES_DIR)

pipeline: fetch dbt-run dbt-test
	@printf "\nPipeline complete ✓\n"

test:
	pytest tests/ -v

bronze-ls:
	@printf "Bronze snapshots:\n"
	@find data/bronze -name "*.json" 2>/dev/null | sort | while read f; do \
		records=$$(python -c "import json; d=json.load(open('$$f')); print(d['record_count'])"); \
		printf "  %-55s %s records\n" "$$f" "$$records"; \
	done || printf "  (tidak ada bronze snapshot)\n"

status:
	@python - <<'PYEOF'
	import duckdb, sys
	try:
	    con = duckdb.connect("warehouse/bmkg.duckdb")
	    row = con.execute("""
	        SELECT run_time, total_records, max_magnitude,
	               freshness_lag_hours, pipeline_status,
	               freshness_status, anomaly_status, quality_status
	        FROM earthquake_quality_summary
	    """).fetchone()
	    print(f"\n  run_time          : {row[0]}")
	    print(f"  total_records     : {row[1]}")
	    print(f"  max_magnitude     : {row[2]}")
	    print(f"  freshness_lag     : {row[3]:.2f}h")
	    print(f"  pipeline_status   : {row[4]}")
	    print(f"  freshness_status  : {row[5]}")
	    print(f"  anomaly_status    : {row[6]}")
	    print(f"  quality_status    : {row[7]}\n")
	except Exception as e:
	    print(f"  Error: {e} — jalankan 'make pipeline' dulu.\n")
	PYEOF

clean:
	rm -rf warehouse/
	rm -rf $(DBT_DIR)/target/ $(DBT_DIR)/dbt_packages/
	rm -rf .pytest_cache/
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@printf "Cleaned.\n"
