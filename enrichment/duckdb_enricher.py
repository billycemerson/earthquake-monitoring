"""
DuckDB enrichment module for pipeline data.

Enriches staging earthquake data with province information by reverse
geocoding latitude and longitude coordinates.
"""

import logging
from pathlib import Path
from typing import List, Tuple, Optional

import duckdb

from enrichment.geocoder import ProvinceGeocoder

log = logging.getLogger(__name__)


class DuckDBEnricher:
    """
    Enriches earthquake data in DuckDB warehouse with province information.
    """

    def __init__(self, warehouse_path: Path = Path("warehouse/bmkg.duckdb")):
        """
        Initialize DuckDB enricher.
        
        Args:
            warehouse_path: Path to DuckDB warehouse file
        """
        self.warehouse_path = warehouse_path
        self.geocoder = ProvinceGeocoder()

    def ensure_warehouse_exists(self) -> None:
        """Verify warehouse file exists."""
        if not self.warehouse_path.exists():
            raise FileNotFoundError(f"Warehouse not found: {self.warehouse_path}")

    def add_province_column(self) -> None:
        """
        Add province column to staging table if it doesn't exist.
        """
        con = duckdb.connect(str(self.warehouse_path))
        try:
            con.execute("""
                ALTER TABLE stg_earthquake 
                ADD COLUMN IF NOT EXISTS province VARCHAR
            """)
            log.info("Province column ensured in stg_earthquake")
        except Exception as e:
            log.error(f"Error adding province column: {e}")
        finally:
            con.close()

    def fetch_records_to_enrich(self) -> List[Tuple]:
        """
        Fetch earthquake records that need province enrichment.
        
        Returns:
            List of (event_id, latitude, longitude) tuples
        """
        con = duckdb.connect(str(self.warehouse_path))
        try:
            rows = con.execute("""
                SELECT event_id, latitude, longitude
                FROM staging.stg_earthquake
                WHERE (province IS NULL OR province = '')
                AND latitude IS NOT NULL 
                AND longitude IS NOT NULL
                ORDER BY event_id
            """).fetchall()
            return rows
        finally:
            con.close()

    def enrich_pipeline_data(self) -> int:
        """
        Enrich pipeline data with province information.
        
        Returns:
            Number of records enriched
        """
        self.ensure_warehouse_exists()
        self.add_province_column()

        records = self.fetch_records_to_enrich()
        
        if not records:
            log.info("No records to enrich in pipeline")
            return 0

        log.info(f"Enriching {len(records)} records from pipeline")

        # Extract coordinates
        coordinates = [(lat, lon) for _, lat, lon in records]

        # Batch geocode
        try:
            provinces = self.geocoder.geocode_batch(coordinates)
        except Exception as e:
            log.error(f"Geocoding failed: {e}")
            return 0

        # Prepare updates
        updates = [
            (province, event_id)
            for (event_id, _, _), province in zip(records, provinces)
        ]

        # Bulk update
        con = duckdb.connect(str(self.warehouse_path))
        try:
            con.executemany(
                """
                UPDATE stg_earthquake 
                SET province = ? 
                WHERE event_id = ?
                """,
                updates
            )
            log.info(f"Successfully enriched {len(updates)} records")
            return len(updates)
        except Exception as e:
            log.error(f"Error updating records: {e}")
            return 0
        finally:
            con.close()

    def get_enrichment_stats(self) -> dict:
        """
        Get statistics on province enrichment status.
        
        Returns:
            Dictionary with enrichment statistics
        """
        con = duckdb.connect(str(self.warehouse_path))
        try:
            total = con.execute(
                "SELECT COUNT(*) FROM staging.stg_earthquake"
            ).fetchone()[0]
            
            enriched = con.execute(
                "SELECT COUNT(*) FROM staging.stg_earthquake WHERE province IS NOT NULL AND province != ''"
            ).fetchone()[0]
            
            pending = total - enriched
            
            return {
                'total_records': total,
                'enriched_records': enriched,
                'pending_records': pending,
                'enrichment_percentage': (enriched / total * 100) if total > 0 else 0
            }
        except Exception as e:
            log.error(f"Error fetching enrichment stats: {e}")
            return {}
        finally:
            con.close()