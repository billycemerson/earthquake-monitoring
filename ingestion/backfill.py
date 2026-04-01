"""
backfill.py — Replay bronze snapshots ke DuckDB
-------------------------------------------------
Gunakan saat warehouse corrupt / skema berubah / perlu rebuild.

Usage:
  python ingestion/backfill.py
  python ingestion/backfill.py --from 2026-03-01 --to 2026-03-31
  python ingestion/backfill.py --dry-run
"""

import argparse, hashlib, json, logging
from datetime import date, datetime, timezone
from pathlib import Path
import duckdb

BRONZE_BASE    = Path("data/bronze")
WAREHOUSE_PATH = Path("warehouse/bmkg.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="date_from", type=date.fromisoformat, default=None)
    p.add_argument("--to",   dest="date_to",   type=date.fromisoformat, default=None)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def discover_partitions(date_from=None, date_to=None):
    partitions = sorted(BRONZE_BASE.glob("ingestion_date=*/earthquakes.json"))
    result = []
    for path in partitions:
        d = date.fromisoformat(path.parent.name.replace("ingestion_date=", ""))
        if date_from and d < date_from: continue
        if date_to   and d > date_to:   continue
        result.append((d, path))
    return result


def load_partition(con, partition_date, bronze_path):
    with open(bronze_path, encoding="utf-8") as f:
        envelope = json.load(f)
    rows = []
    for row in envelope["records"]:
        event_id = hashlib.sha256((row["DateTime"] + row["Coordinates"]).encode()).hexdigest()[:16]
        try:    depth_km = float(row["Kedalaman"].replace(" km", "").strip())
        except: depth_km = None
        rows.append((event_id, row["DateTime"], row["Coordinates"], float(row["Magnitude"]),
                     depth_km, row.get("Wilayah",""), row.get("Dirasakan",""),
                     envelope["ingestion_time"], partition_date.isoformat(), str(bronze_path)))
    con.executemany("INSERT OR REPLACE INTO raw_earthquake VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def main():
    args = parse_args()
    partitions = discover_partitions(args.date_from, args.date_to)
    if not partitions:
        log.info("Tidak ada partisi ditemukan.")
        return
    log.info(f"Ditemukan {len(partitions)} partisi:")
    for d, p in partitions:
        log.info(f"  {d} → {p}")
    if args.dry_run:
        log.info("Dry run — tidak ada yang dieksekusi.")
        return
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE_PATH))
    con.execute("""CREATE TABLE IF NOT EXISTS raw_earthquake (
        event_id VARCHAR PRIMARY KEY, datetime TIMESTAMPTZ, coordinates VARCHAR,
        magnitude DOUBLE, depth_km DOUBLE, wilayah VARCHAR, dirasakan VARCHAR,
        ingestion_time TIMESTAMPTZ, ingestion_date DATE, source_file VARCHAR)""")
    total = sum(load_partition(con, d, p) for d, p in partitions)
    log.info(f"Backfill selesai. Total rows diproses: {total}")
    con.close()

if __name__ == "__main__":
    main()
