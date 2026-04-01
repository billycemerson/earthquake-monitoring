import requests
import duckdb
import hashlib
from datetime import datetime, timezone

URL = "https://data.bmkg.go.id/DataMKG/TEWS/gempadirasakan.json"

res = requests.get(URL)
data = res.json()["Infogempa"]["gempa"]

con = duckdb.connect("warehouse/bmkg.duckdb")

con.execute("""
CREATE TABLE IF NOT EXISTS raw_earthquake (
    event_id VARCHAR PRIMARY KEY,
    datetime TIMESTAMPTZ,
    coordinates VARCHAR,
    magnitude DOUBLE,
    depth_km DOUBLE,
    wilayah VARCHAR,
    dirasakan VARCHAR,
    ingestion_time TIMESTAMPTZ
)
""")

rows = []

for row in data:
    event_hash = hashlib.md5(
        (row["DateTime"] + row["Coordinates"]).encode()
    ).hexdigest()

    depth = float(row["Kedalaman"].replace(" km", ""))

    rows.append((
        event_hash,
        row["DateTime"],
        row["Coordinates"],
        float(row["Magnitude"]),
        depth,
        row["Wilayah"],
        row["Dirasakan"],
        datetime.now(timezone.utc).isoformat()
    ))

con.executemany("""
INSERT OR REPLACE INTO raw_earthquake
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", rows)

con.close()
