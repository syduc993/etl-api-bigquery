"""
Script để sync bill data hàng ngày (ngày hôm qua - n-1).
Script này tự động tính ngày hôm qua theo timezone VN (UTC+7) và sync toàn bộ pipeline.

Usage:
    python -m src.features.nhanh.bills.scripts.daily_bills_sync
    
Hoặc chạy trực tiếp:
    python src/features/nhanh/bills/scripts/daily_bills_sync.py
"""
import sys
import os
from datetime import datetime, timedelta, timezone

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from src.features.nhanh.bills.pipeline import BillPipeline
from src.shared.bigquery import BigQueryExternalTableSetup
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """
    Main function: Sync bill data cho ngày hôm qua (n-1).
    Chạy full pipeline: Extract → Load → Setup External Tables → Transform
    """
    try:
        logger.info("=" * 60)
        logger.info("Starting Daily Bills Sync Pipeline")
        logger.info("=" * 60)
        
        # Calculate Yesterday (VN Time)
        # VN is UTC+7
        vn_tz = timezone(timedelta(hours=7))
        now_vn = datetime.now(vn_tz)
        yesterday_vn = now_vn.date() - timedelta(days=1)
        
        from_date = datetime.combine(yesterday_vn, datetime.min.time()).replace(tzinfo=vn_tz)
        to_date = datetime.combine(yesterday_vn, datetime.max.time()).replace(tzinfo=vn_tz)
        
        logger.info(f"Target Date (n-1): {yesterday_vn}")
        logger.info(f"Date Range: {from_date} to {to_date}")
        
        # Khởi tạo pipeline
        pipeline = BillPipeline()
        
        # Step 1: Extract và Load (Bronze layer)
        logger.info("Step 1: Extracting and loading bills to GCS...")
        extract_result = pipeline.run_extract_load(
            from_date=from_date,
            to_date=to_date,
            process_by_day=True
        )
        
        logger.info("✅ Step 1 completed: Data extracted and loaded to GCS")
        logger.info(f"   Bills extracted: {extract_result.get('bills_extracted', 0)}")
        logger.info(f"   Products extracted: {extract_result.get('products_extracted', 0)}")
        logger.info(f"   Days processed: {extract_result.get('days_processed', 0)}")
        
        # Step 2: Setup BigQuery External Tables
        logger.info("Step 2: Setting up BigQuery External Tables...")
        bq_setup = BigQueryExternalTableSetup()
        bq_setup.setup_all_tables(platforms=["nhanh"])
        logger.info("✅ Step 2 completed: External Tables updated")
        
        # Step 3: Transform từ Bronze → Fact Tables
        logger.info("Step 3: Transforming data to fact tables...")
        transform_result = pipeline.run_transform()
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

