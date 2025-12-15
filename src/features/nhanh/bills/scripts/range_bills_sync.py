"""
Script để sync bill data theo khoảng ngày (date range).
Script này cho phép chỉ định from_date và to_date để sync dữ liệu.

Usage:
    # Sync từ ngày cụ thể đến ngày cụ thể
    python -m src.features.nhanh.bills.scripts.range_bills_sync 2025-11-01 2025-11-30
    
    # Sync cho một ngày cụ thể
    python -m src.features.nhanh.bills.scripts.range_bills_sync 2025-11-29 2025-11-29
    
    # Sync từ ngày cụ thể đến hôm nay
    python -m src.features.nhanh.bills.scripts.range_bills_sync 2025-11-01
    
Hoặc chạy trực tiếp:
    python src/features/nhanh/bills/scripts/range_bills_sync.py 2025-11-01 2025-11-30
"""
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from src.features.nhanh.bills.pipeline import BillPipeline
from src.shared.logging import get_logger

logger = get_logger(__name__)


def parse_date(date_str: str) -> datetime:
    """
    Parse date string thành datetime với timezone VN.
    
    Args:
        date_str: Date string format YYYY-MM-DD
        
    Returns:
        datetime với timezone VN (UTC+7)
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        vn_tz = timezone(timedelta(hours=7))
        return datetime.combine(date_obj, datetime.min.time()).replace(tzinfo=vn_tz)
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD format (e.g., 2025-11-29)")


def main():
    """
    Main function: Sync bill data theo khoảng ngày.
    Chạy full pipeline: Extract → Load (flatten integrated in loader)
    """
    try:
        logger.info("=" * 60)
        logger.info("Starting Range Bills Sync Pipeline")
        logger.info("=" * 60)
        
        # Parse arguments
        if len(sys.argv) < 2:
            logger.error("Missing required arguments. Usage:")
            logger.error("  python -m src.features.nhanh.bills.scripts.range_bills_sync <from_date> [to_date]")
            logger.error("  Example: python -m src.features.nhanh.bills.scripts.range_bills_sync 2025-11-01 2025-11-30")
            sys.exit(1)
        
        from_date_str = sys.argv[1]
        from_date = parse_date(from_date_str)
        
        # Nếu có to_date thì dùng, không thì dùng hôm nay
        if len(sys.argv) >= 3:
            to_date_str = sys.argv[2]
            to_date = parse_date(to_date_str)
            # Set to end of day
            to_date = datetime.combine(to_date.date(), datetime.max.time()).replace(tzinfo=to_date.tzinfo)
        else:
            # Default: hôm nay (VN timezone)
            vn_tz = timezone(timedelta(hours=7))
            today_vn = datetime.now(vn_tz).date()
            to_date = datetime.combine(today_vn, datetime.max.time()).replace(tzinfo=vn_tz)
            logger.info(f"No to_date provided, using today: {today_vn}")
        
        logger.info(f"Date Range: {from_date.date()} to {to_date.date()}")
        logger.info(f"Full Range: {from_date} to {to_date}")
        
        # Validate date range
        if from_date > to_date:
            logger.error(f"Invalid date range: from_date ({from_date.date()}) must be <= to_date ({to_date.date()})")
            sys.exit(1)
        
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
        
        # Note: Flatten đã được tích hợp vào loader, data được load trực tiếp vào fact tables
        # Không cần setup external tables và transform step nữa
        
        logger.info("=" * 60)
        logger.info("✅ Range Bills Sync Pipeline Completed Successfully!")
        logger.info("=" * 60)
        
        return 0
        
    except ValueError as e:
        logger.error(f"❌ Invalid input: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Range sync failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())

