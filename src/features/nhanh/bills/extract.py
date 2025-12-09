"""
Entry point for extracting Nhanh.vn bills data.
"""
import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

from src.config import settings
from src.shared.logging import get_logger
from src.shared.gcs import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.shared.bigquery import BigQueryExternalTableSetup
from src.pipeline.extraction import extract_entity

logger = get_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Extract Nhanh.vn Bills")
    parser.add_argument("--from-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    from_date = None
    to_date = None
    
    if args.from_date and args.to_date:
        try:
            from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
            to_date = datetime.strptime(args.to_date, "%Y-%m-%d")
            to_date = to_date.replace(hour=23, minute=59, second=59)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)
    elif args.from_date or args.to_date:
        logger.error("Both --from-date and --to-date must be provided.")
        sys.exit(1)
        
    try:
        logger.info("Starting Nhanh Bills Extraction...")
        
        loader = GCSLoader(bucket_name=settings.bronze_bucket)
        watermark_tracker = WatermarkTracker()
        
        extract_entity(
            platform="nhanh",
            entity="bills",
            loader=loader,
            watermark_tracker=watermark_tracker,
            incremental=not (from_date and to_date),
            from_date=from_date,
            to_date=to_date
        )
        
        logger.info("Setting up BigQuery External Tables...")
        bq_setup = BigQueryExternalTableSetup()
        bq_setup.setup_all_tables(platforms=["nhanh"])
        
        logger.info("Done!")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

