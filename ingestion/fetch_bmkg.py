"""
BMKG Earthquake Ingestion — v2 Step 1: Bronze Layer
----------------------------------------------------
Upgrade dari v1:
- Raw API response disimpan ke data/bronze/ DULU sebagai JSON snapshot
- Partisi per tanggal: ingestion_date=YYYY-MM-DD/
- DuckDB di-load DARI file bronze, bukan langsung dari API
- Idempotent: run 2x → hasil sama (file di-overwrite, INSERT OR REPLACE)
"""

import sys
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

BMKG_URL      = "https://data.bmkg.go.id/DataMKG/TEWS/gempadirasakan.json"
BRONZE_BASE   = Path("data/bronze")
WAREHOUSE_PATH = Path("warehouse/bmkg.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def fetch_from_api() -> list:
    log.info(f"Fetching: {BMKG_URL}")
    try:
        res = requests.get(BMKG_URL, timeout=30)
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching data: {e}")
        sys.exit(1)
        
    gempa = res.json().get("Infogempa", {}).get("gempa", [])
    log.info(f"Fetched {len(gempa)} records")
    return gempa


def save_to_bronze(records: list, ingestion_time: datetime) -> Path:
    """Simpan raw API response ke bronze layer (partisi per hari)."""
    date_str = ingestion_time.strftime("%Y-%m-%d")
    partition_dir = BRONZE_BASE / f"ingestion_date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / "earthquakes.json"
    envelope = {
        "ingestion_time": ingestion_time.isoformat(),
        "source_url": BMKG_URL,
        "record_count": len(records),
        "records": records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, indent=2)

    log.info(f"Bronze snapshot saved → {output_path}")
    return output_path


def load_into_duckdb(bronze_path: Path, ingestion_time: datetime) -> None:
    """Load dari file bronze (bukan langsung API) → DuckDB."""
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE_PATH))

    # Skema baru: tambah ingestion_date + source_file untuk audit trail
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_earthquake (
            event_id        VARCHAR PRIMARY KEY,
            datetime        TIMESTAMPTZ,
            coordinates     VARCHAR,
            magnitude       DOUBLE,
            depth_km        DOUBLE,
            wilayah         VARCHAR,
            dirasakan       VARCHAR,
            ingestion_time  TIMESTAMPTZ,
            ingestion_date  DATE,
            source_file     VARCHAR
        )
    """)

    with open(bronze_path, encoding="utf-8") as f:
        envelope = json.load(f)

    rows = []
    for row in envelope["records"]:
        event_id = hashlib.sha256(
            (row["DateTime"] + row["Coordinates"]).encode()
        ).hexdigest()[:16]

        try:
            depth_km = float(row["Kedalaman"].replace(" km", "").strip())
        except (ValueError, AttributeError):
            depth_km = None

        rows.append((
            event_id,
            row["DateTime"],
            row["Coordinates"],
            float(row["Magnitude"]),
            depth_km,
            row.get("Wilayah", ""),
            row.get("Dirasakan", ""),
            ingestion_time.isoformat(),
            ingestion_time.date().isoformat(),
            str(bronze_path),
        ))

    con.executemany("INSERT OR REPLACE INTO raw_earthquake VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    count = con.execute("SELECT COUNT(*) FROM raw_earthquake").fetchone()[0]
    log.info(f"DuckDB total rows: {count}")
    con.close()


def main():
    now = datetime.now(timezone.utc)
    records    = fetch_from_api()
    bronze_path = save_to_bronze(records, now)
    load_into_duckdb(bronze_path, now)
    log.info("Ingestion complete ✓")

if __name__ == "__main__":
    main()
