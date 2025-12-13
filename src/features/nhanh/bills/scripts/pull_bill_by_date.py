"""
Script để kéo dữ liệu bill cho một ngày cụ thể.
Usage: python -m src.features.nhanh.bills.scripts.pull_bill_by_date 2025-11-29
"""
import sys
import os
from datetime import datetime, timezone, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from src.features.nhanh.bills.pipeline import BillPipeline
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """Main function: Extract và Load bill data cho ngày cụ thể."""
    try:
        # Parse date từ command line hoặc sử dụng ngày mặc định
        if len(sys.argv) > 1:
            date_str = sys.argv[1]
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")
                sys.exit(1)
        else:
            # Default: 29/11/2025
            target_date = datetime(2025, 11, 29).date()
        
        logger.info("=" * 60)
        logger.info(f"Starting Bill Data Extraction for Date: {target_date}")
        logger.info("=" * 60)
        
        # Setup timezone VN (UTC+7)
        vn_tz = timezone(timedelta(hours=7))
        
        # Tạo datetime range cho cả ngày (00:00:00 đến 23:59:59)
        from_date = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=vn_tz)
        to_date = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=vn_tz)
        
        logger.info(f"Date Range: {from_date} to {to_date}")
        
        # Khởi tạo pipeline
        pipeline = BillPipeline()
        
        # Chạy Extract và Load
        result = pipeline.run_extract_load(
            from_date=from_date,
            to_date=to_date,
            process_by_day=True
        )
        
        logger.info("=" * 60)
        logger.info("✅ Bill Data Extraction Completed Successfully!")
        logger.info(f"   Bills extracted: {result.get('bills_extracted', 0)}")
        logger.info(f"   Products extracted: {result.get('products_extracted', 0)}")
        logger.info(f"   Days processed: {result.get('days_processed', 0)}")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())

