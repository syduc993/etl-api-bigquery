"""
Entry point cho transform khi được trigger từ Eventarc/GCS events.
Script này setup external tables và chạy transform step.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.features.nhanh.bills.components.transformer import BillTransformer
from src.shared.bigquery import BigQueryExternalTableSetup
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run transform step (triggered by GCS events)."""
    try:
        logger.info("🔄 Transform triggered by GCS event")
        
        # Step 1: Setup External Tables (CRITICAL: Refresh external tables to see new files)
        logger.info("Step 1: Setting up/updating BigQuery External Tables...")
        bq_setup = BigQueryExternalTableSetup()
        bq_setup.setup_all_tables(platforms=["nhanh"])
        logger.info("✅ Step 1 completed: External Tables updated")
        
        # Step 2: Transform from Bronze External Tables to Fact Tables
        logger.info("Step 2: Starting transform from bronze external tables to fact tables...")
        transformer = BillTransformer()
        result = transformer.transform_flatten()
        
        logger.info(
            "✅ Transform completed successfully",
            job_id=result.get('job_id'),
            status=result.get('status')
        )
        
        print("✅ Transform completed!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Transform failed: {e}", exc_info=True)
        print(f"❌ Transform failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

