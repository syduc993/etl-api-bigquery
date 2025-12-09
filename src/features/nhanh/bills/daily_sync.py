"""
Daily sync script: Extract từ Nhanh API → GCS → Transform vào fact tables.
Script này chạy incremental extraction và transform vào fact tables.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

from src.config import settings
from src.shared.logging import get_logger
from src.shared.gcs import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.shared.bigquery import BigQueryExternalTableSetup
from src.pipeline.extraction import extract_entity
from src.features.nhanh.bills.transformer import BillTransformer

logger = get_logger(__name__)

def main():
    """Main function: Extract → Load → Transform."""
    try:
        logger.info("=" * 60)
        logger.info("Starting Daily Bills Sync Pipeline")
        logger.info("=" * 60)
        
        # Step 1: Extract từ Nhanh API → GCS (Bronze)
        logger.info("Step 1: Extracting bills from Nhanh API...")
        loader = GCSLoader(bucket_name=settings.bronze_bucket)
        watermark_tracker = WatermarkTracker()
        
        extract_entity(
            platform="nhanh",
            entity="bills",
            loader=loader,
            watermark_tracker=watermark_tracker,
            incremental=True,  # Incremental extraction
            from_date=None,
            to_date=None
        )
        
        logger.info("✅ Step 1 completed: Data extracted to GCS")
        
        # Step 2: Setup BigQuery External Tables
        logger.info("Step 2: Setting up BigQuery External Tables...")
        bq_setup = BigQueryExternalTableSetup()
        bq_setup.setup_all_tables(platforms=["nhanh"])
        logger.info("✅ Step 2 completed: External Tables updated")
        
        # Step 3: Transform từ Bronze → Fact Tables
        logger.info("Step 3: Transforming data to fact tables...")
        transformer = BillTransformer()
        transform_result = transformer.transform_flatten()
        logger.info(f"✅ Step 3 completed: Transform completed. Job ID: {transform_result.get('job_id')}")
        
        logger.info("=" * 60)
        logger.info("✅ Daily Bills Sync Pipeline Completed Successfully!")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Daily sync failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())

