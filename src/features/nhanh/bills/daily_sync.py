"""
Daily sync script: Extract từ Nhanh API → Flatten và Load trực tiếp vào fact tables.
Script này chạy extraction, flatten trong Python, và load trực tiếp vào fact tables.
Không cần transform step nữa vì flatten đã được tích hợp vào loader.
"""
import sys
import os
from datetime import datetime, timedelta, timezone

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

from src.shared.logging import get_logger
from src.features.nhanh.bills.pipeline import BillPipeline

logger = get_logger(__name__)

def main():
    """Main function: Extract → Flatten → Load (all in one pipeline)."""
    try:
        logger.info("=" * 60)
        logger.info("Starting Daily Bills Sync Pipeline")
        logger.info("=" * 60)
        
        # Calculate Yesterday (VN Time)
        # Helper to ensure we get the correct "n-1" day regardless of where this script runs (UTC or otherwise)
        # VN is UTC+7
        vn_tz = timezone(timedelta(hours=7))
        now_vn = datetime.now(vn_tz)
        yesterday_vn = now_vn.date() - timedelta(days=1)
        
        from_date = datetime.combine(yesterday_vn, datetime.min.time()).replace(tzinfo=vn_tz)
        to_date = datetime.combine(yesterday_vn, datetime.max.time()).replace(tzinfo=vn_tz)
        
        logger.info(f"Target Date (n-1): {yesterday_vn}")

        # Run Extract + Load pipeline
        # Loader sẽ tự động flatten nested structures và load trực tiếp vào fact tables
        logger.info("Running Extract + Load pipeline (flatten integrated in loader)...")
        pipeline = BillPipeline()
        
        result = pipeline.run_extract_load(
            from_date=from_date,
            to_date=to_date,
            process_by_day=True
        )
        
        logger.info("=" * 60)
        logger.info("✅ Daily Bills Sync Pipeline Completed Successfully!")
        logger.info(f"   - Bills extracted: {result.get('bills_extracted', 0)}")
        logger.info(f"   - Products extracted: {result.get('products_extracted', 0)}")
        logger.info(f"   - Days processed: {result.get('days_processed', 0)}")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Daily sync failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())

