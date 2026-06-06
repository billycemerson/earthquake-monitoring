"""
Main enrichment script for province data.

Orchestrates enrichment of earthquake data in both DuckDB pipeline and
Google Sheets with province information derived from coordinates.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from enrichment.duckdb_enricher import DuckDBEnricher
from enrichment.sheet_enricher import SheetEnricher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def enrich_pipeline(warehouse_path: Path = Path("warehouse/bmkg.duckdb")) -> int:
    """
    Enrich pipeline data in DuckDB.
    
    Args:
        warehouse_path: Path to DuckDB warehouse
        
    Returns:
        Number of records enriched
    """
    log.info("Starting pipeline enrichment")
    
    try:
        enricher = DuckDBEnricher(warehouse_path)
        count = enricher.enrich_pipeline_data()
        
        stats = enricher.get_enrichment_stats()
        log.info(f"Pipeline enrichment stats: {stats}")
        
        return count
    except Exception as e:
        log.error(f"Pipeline enrichment failed: {e}")
        return 0


def enrich_sheet(
    sheet_id: str,
    sheet_name: str = "staging_data",
    credentials_file: str = "service_account.json"
) -> int:
    """
    Enrich Google Sheet data.
    
    Args:
        sheet_id: Google Sheet ID
        sheet_name: Worksheet name
        credentials_file: Path to service account credentials
        
    Returns:
        Number of records enriched
    """
    log.info("Starting sheet enrichment")
    
    try:
        enricher = SheetEnricher(sheet_id, sheet_name, credentials_file)
        count = enricher.enrich_sheet_data()
        return count
    except Exception as e:
        log.error(f"Sheet enrichment failed: {e}")
        return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enrich earthquake data with province information"
    )
    parser.add_argument(
        '--pipeline',
        action='store_true',
        default=False,
        help='Enrich pipeline data (DuckDB)'
    )
    parser.add_argument(
        '--sheet',
        type=str,
        default=None,
        help='Enrich Google Sheet (provide sheet ID)'
    )
    parser.add_argument(
        '--sheet-name',
        type=str,
        default='staging_data',
        help='Worksheet name in Google Sheet'
    )
    parser.add_argument(
        '--credentials',
        type=str,
        default='service_account.json',
        help='Path to service account credentials file'
    )
    parser.add_argument(
        '--warehouse',
        type=Path,
        default=Path('warehouse/bmkg.duckdb'),
        help='Path to DuckDB warehouse'
    )
    parser.add_argument(
        '--report',
        action='store_true',
        default=False,
        help='Generate enrichment report for sheet'
    )
    parser.add_argument(
        '--report-file',
        type=str,
        default='enrichment_report.csv',
        help='Output file for enrichment report'
    )

    args = parser.parse_args()

    # If no specific action, enrich both
    if not args.pipeline and not args.sheet and not args.report:
        args.pipeline = True

    total_enriched = 0

    # Enrich pipeline
    if args.pipeline:
        count = enrich_pipeline(args.warehouse)
        total_enriched += count

    # Enrich sheet
    if args.sheet:
        count = enrich_sheet(args.sheet, args.sheet_name, args.credentials)
        total_enriched += count

    # Generate report
    if args.report and args.sheet:
        try:
            enricher = SheetEnricher(args.sheet, args.sheet_name, args.credentials)
            enricher.generate_enrichment_report(args.report_file)
        except Exception as e:
            log.error(f"Report generation failed: {e}")

    log.info(f"Enrichment complete. Total records enriched: {total_enriched}")
    
    if total_enriched > 0:
        sys.exit(0)
    else:
        sys.exit(0)  # Don't fail if nothing to enrich


if __name__ == "__main__":
    main()