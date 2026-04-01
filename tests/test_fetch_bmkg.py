"""
Unit tests untuk ingestion/fetch_bmkg.py

Run:
  pytest tests/ -v
"""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

SAMPLE_API_RESPONSE = {
    "Infogempa": {
        "gempa": [
            {
                "DateTime": "2026-04-01T06:00:00+07:00",
                "Coordinates": "-7.5,110.3",
                "Magnitude": "5.2",
                "Kedalaman": "15 km",
                "Wilayah": "10 km Barat Laut Yogyakarta",
                "Dirasakan": "III Yogyakarta",
            },
            {
                "DateTime": "2026-04-01T04:30:00+07:00",
                "Coordinates": "-8.1,115.2",
                "Magnitude": "4.7",
                "Kedalaman": "82 km",
                "Wilayah": "20 km Timur Denpasar",
                "Dirasakan": "II Denpasar",
            },
        ]
    }
}


class TestFetchFromApi:
    def test_returns_list_of_records(self):
        from ingestion.fetch_bmkg import fetch_from_api
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_API_RESPONSE
        mock_resp.raise_for_status.return_value = None
        with patch("ingestion.fetch_bmkg.requests.get", return_value=mock_resp):
            result = fetch_from_api()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_exits_on_api_failure(self):
        from ingestion.fetch_bmkg import fetch_from_api
        import requests as req
        with patch("ingestion.fetch_bmkg.requests.get", side_effect=req.ConnectionError):
            with pytest.raises(SystemExit):
                fetch_from_api()


class TestSaveToBronze:
    def test_creates_partitioned_file(self, tmp_path, monkeypatch):
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        path = fetch_bmkg.save_to_bronze(records, ts)
        assert path.exists()
        assert "ingestion_date=2026-04-01" in str(path)

    def test_envelope_structure(self, tmp_path, monkeypatch):
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        path = fetch_bmkg.save_to_bronze(records, ts)
        with open(path) as f:
            envelope = json.load(f)
        assert "ingestion_time" in envelope
        assert "source_url" in envelope
        assert envelope["record_count"] == 2

    def test_idempotent_overwrite(self, tmp_path, monkeypatch):
        """Run 2x dengan data sama → file yang sama, bukan duplikat."""
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        path1 = fetch_bmkg.save_to_bronze(records, ts)
        path2 = fetch_bmkg.save_to_bronze(records, ts)
        assert path1 == path2
        with open(path2) as f:
            assert json.load(f)["record_count"] == 2


class TestEventId:
    def test_stable_key(self):
        row = SAMPLE_API_RESPONSE["Infogempa"]["gempa"][0]
        k1 = hashlib.sha256((row["DateTime"] + row["Coordinates"]).encode()).hexdigest()[:16]
        k2 = hashlib.sha256((row["DateTime"] + row["Coordinates"]).encode()).hexdigest()[:16]
        assert k1 == k2

    def test_unique_key_per_event(self):
        rows = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ids = [hashlib.sha256((r["DateTime"]+r["Coordinates"]).encode()).hexdigest()[:16] for r in rows]
        assert len(set(ids)) == len(ids)


class TestLoadIntoDuckdb:
    def test_loads_records_correctly(self, tmp_path, monkeypatch):
        import duckdb
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        monkeypatch.setattr(fetch_bmkg, "WAREHOUSE_PATH", tmp_path / "test.duckdb")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        bronze_path = fetch_bmkg.save_to_bronze(records, ts)
        fetch_bmkg.load_into_duckdb(bronze_path, ts)
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        assert con.execute("SELECT COUNT(*) FROM raw_earthquake").fetchone()[0] == 2

    def test_idempotent_double_load(self, tmp_path, monkeypatch):
        """Load 2x data sama → tetap 2 rows, bukan 4."""
        import duckdb
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        monkeypatch.setattr(fetch_bmkg, "WAREHOUSE_PATH", tmp_path / "test.duckdb")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        bronze_path = fetch_bmkg.save_to_bronze(records, ts)
        fetch_bmkg.load_into_duckdb(bronze_path, ts)
        fetch_bmkg.load_into_duckdb(bronze_path, ts)
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        assert con.execute("SELECT COUNT(*) FROM raw_earthquake").fetchone()[0] == 2

    def test_depth_parsing(self, tmp_path, monkeypatch):
        import duckdb
        from ingestion import fetch_bmkg
        monkeypatch.setattr(fetch_bmkg, "BRONZE_BASE", tmp_path / "bronze")
        monkeypatch.setattr(fetch_bmkg, "WAREHOUSE_PATH", tmp_path / "test.duckdb")
        records = SAMPLE_API_RESPONSE["Infogempa"]["gempa"]
        ts = datetime(2026, 4, 1, 6, 0, 0, tzinfo=timezone.utc)
        bronze_path = fetch_bmkg.save_to_bronze(records, ts)
        fetch_bmkg.load_into_duckdb(bronze_path, ts)
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        depths = con.execute("SELECT depth_km FROM raw_earthquake ORDER BY depth_km").fetchall()
        assert depths[0][0] == 15.0
        assert depths[1][0] == 82.0
