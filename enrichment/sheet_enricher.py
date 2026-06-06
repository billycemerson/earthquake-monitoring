"""
Google Sheet enrichment module for historical data.

Enriches historical earthquake data stored in Google Sheets with province
information by reverse geocoding coordinates.
"""

import logging
import os
from typing import List, Dict, Optional, Tuple

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from enrichment.geocoder import ProvinceGeocoder

log = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


class SheetEnricher:
    """
    Enriches earthquake data in Google Sheets with province information.
    """

    def __init__(
        self,
        sheet_id: str,
        sheet_name: str = "staging_data",
        credentials_file: Optional[str] = None
    ):
        """
        Initialize Sheet enricher.
        
        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the worksheet
            credentials_file: Path to service account credentials JSON
        """
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.credentials_file = credentials_file or "service_account.json"
        self.geocoder = ProvinceGeocoder()
        self.gc = None
        self.sheet = None

    def connect(self) -> None:
        """Establish connection to Google Sheets."""
        try:
            if not os.path.exists(self.credentials_file):
                raise FileNotFoundError(
                    f"Credentials file not found: {self.credentials_file}"
                )

            credentials = Credentials.from_service_account_file(
                self.credentials_file,
                scopes=SCOPES
            )
            self.gc = gspread.authorize(credentials)
            self.sheet = self.gc.open_by_key(self.sheet_id).worksheet(self.sheet_name)
            log.info(f"Connected to sheet: {self.sheet_name}")
        except Exception as e:
            log.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def fetch_records(self) -> List[Dict]:
        """
        Fetch all records from sheet.
        
        Returns:
            List of record dictionaries
        """
        if not self.sheet:
            raise RuntimeError("Not connected to sheet. Call connect() first.")

        try:
            records = self.sheet.get_all_records()
            log.info(f"Fetched {len(records)} records from sheet")
            return records
        except Exception as e:
            log.error(f"Error fetching records: {e}")
            raise

    def ensure_province_column(self) -> int:
        """
        Ensure province column exists in sheet.
        
        Returns:
            Column index (1-based) for province column
        """
        if not self.sheet:
            raise RuntimeError("Not connected to sheet. Call connect() first.")

        try:
            headers = self.sheet.row_values(1)
            
            if 'province' in headers:
                col_index = headers.index('province') + 1
                log.info(f"Province column found at column {col_index}")
                return col_index
            
            # Add province column
            new_col_index = len(headers) + 1
            self.sheet.update_cell(1, new_col_index, 'province')
            log.info(f"Province column added at column {new_col_index}")
            return new_col_index
        except Exception as e:
            log.error(f"Error ensuring province column: {e}")
            raise

    def enrich_sheet_data(self) -> int:
        """
        Enrich sheet data with province information.
        
        Returns:
            Number of records enriched
        """
        self.connect()
        province_col = self.ensure_province_column()

        records = self.fetch_records()
        
        if not records:
            log.warning("No records found in sheet")
            return 0

        # Filter records needing enrichment
        records_to_enrich = []
        row_indices = []

        for idx, record in enumerate(records, start=2):  # Start at row 2 (after header)
            province = record.get('province', '').strip()
            
            if province:
                continue  # Already has province

            latitude = record.get('latitude')
            longitude = record.get('longitude')

            if not self.geocoder.validate_coordinates(latitude, longitude):
                continue

            try:
                records_to_enrich.append((
                    float(latitude),
                    float(longitude)
                ))
                row_indices.append(idx)
            except (ValueError, TypeError):
                log.warning(f"Invalid coordinates at row {idx}")

        if not records_to_enrich:
            log.info("No records to enrich in sheet")
            return 0

        log.info(f"Enriching {len(records_to_enrich)} records in sheet")

        # Batch geocode
        try:
            provinces = self.geocoder.geocode_batch(records_to_enrich)
        except Exception as e:
            log.error(f"Geocoding failed: {e}")
            return 0

        # Update sheet
        try:
            for row_idx, province in zip(row_indices, provinces):
                self.sheet.update_cell(row_idx, province_col, province)
            
            log.info(f"Successfully enriched {len(provinces)} records in sheet")
            return len(provinces)
        except Exception as e:
            log.error(f"Error updating sheet: {e}")
            return 0

    def generate_enrichment_report(self, output_file: str = "enrichment_report.csv") -> None:
        """
        Generate enrichment report and save to CSV.
        
        Args:
            output_file: Path to save the report
        """
        self.connect()
        records = self.fetch_records()

        if not records:
            log.warning("No records to generate report")
            return

        # Prepare data
        report_data = []
        for record in records:
            latitude = record.get('latitude')
            longitude = record.get('longitude')
            province = record.get('province', '').strip()

            if self.geocoder.validate_coordinates(latitude, longitude):
                if not province:
                    suggested_province = self.geocoder.geocode_single(
                        float(latitude),
                        float(longitude)
                    )
                    status = 'AUTO_SUGGESTED'
                else:
                    suggested_province = province
                    status = 'EXISTING'

                report_data.append({
                    'datetime': record.get('datetime'),
                    'latitude': latitude,
                    'longitude': longitude,
                    'wilayah': record.get('wilayah'),
                    'current_province': province,
                    'suggested_province': suggested_province,
                    'status': status
                })

        if report_data:
            df = pd.DataFrame(report_data)
            df.to_csv(output_file, index=False)
            log.info(f"Enrichment report saved to {output_file}")
        else:
            log.warning("No valid coordinates found for report generation")