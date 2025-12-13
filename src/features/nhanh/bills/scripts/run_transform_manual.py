"""
Script để chạy transform manual cho các ngày cụ thể.
Dùng khi schedule không chạy hoặc cần backfill dữ liệu.
"""
import sys
import os
from datetime import date, datetime, timezone, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from src.features.nhanh.bills.components.transformer import BillTransformer
from src.shared.bigquery import BigQueryExternalTableSetup
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """Chạy transform manual."""
    try:
        print("=" * 80)
        print("🔄 CHẠY TRANSFORM MANUAL")
        print("=" * 80)
        print()
        
        # Step 1: Setup External Tables (đảm bảo external tables được update)
        print("📊 Bước 1: Setup/Update BigQuery External Tables...")
        bq_setup = BigQueryExternalTableSetup()
        bq_setup.setup_all_tables(platforms=["nhanh"])
        print("✅ External Tables đã được update")
        print()
        
        # Step 2: Run Transform
        print("🔄 Bước 2: Chạy Transform (Bronze → Fact Tables)...")
        print("   Lưu ý: Transform sẽ MERGE tất cả dữ liệu từ Bronze External Tables")
        print("   vào Fact Tables (không filter theo date)")
        print()
        
        transformer = BillTransformer()
        result = transformer.transform_flatten()
        
        print()
        print("=" * 80)
        print("✅ TRANSFORM HOÀN TẤT!")
        print("=" * 80)
        print(f"Job ID: {result.get('job_id')}")
        print(f"Status: {result.get('status')}")
        print()
        print("💡 Kiểm tra lại dữ liệu bằng:")
        print("   python -m src.features.nhanh.bills.scripts.check_fact_tables_data")
        print("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Transform failed: {e}", exc_info=True)
        print(f"❌ Transform failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

