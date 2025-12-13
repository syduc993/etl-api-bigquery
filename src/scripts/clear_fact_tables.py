"""
Script để xóa hết data trong native fact tables (không xóa table structure).
Dùng khi muốn repopulate từ đầu.
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from google.cloud import bigquery
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def clear_fact_tables(skip_confirm: bool = False):
    """Xóa hết data trong fact tables (TRUNCATE)."""
    client = bigquery.Client(project=settings.gcp_project)
    
    tables = [
        f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0",
        f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0",
    ]
    
    for table_id in tables:
        try:
            # Check current row count first (to verify before/after)
            print(f"\n📊 Checking {table_id}...")
            count_query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
            count_job = client.query(count_query)
            count_result = list(count_job.result())
            before_count = count_result[0].cnt if count_result else 0
            print(f"   Current rows: {before_count:,}")
            
            # Truncate table
            print(f"🗑️  Truncating {table_id}...")
            truncate_query = f"TRUNCATE TABLE `{table_id}`"
            client.query(truncate_query).result()
            
            # Verify deletion
            verify_job = client.query(count_query)
            verify_result = list(verify_job.result())
            after_count = verify_result[0].cnt if verify_result else 0
            
            logger.info(f"✅ Truncated {table_id} ({before_count:,} -> {after_count:,} rows)")
            print(f"   ✅ Truncated: {before_count:,} -> {after_count:,} rows")
            
        except Exception as e:
            logger.error(f"Error truncating {table_id}: {e}")
            print(f"   ❌ Error: {e}")
            raise
    
    print("\n✅ All fact tables have been truncated!")


def main():
    """Main function."""
    import sys
    
    skip_confirm = "--yes" in sys.argv or "-y" in sys.argv
    
    if not skip_confirm:
        print("⚠️  WARNING: This will DELETE ALL DATA from fact tables:")
        print(f"   - {settings.target_dataset}.fact_sales_bills_v3_0")
        print(f"   - {settings.target_dataset}.fact_sales_bills_product_v3_0")
        print("\nPress Enter to continue or Ctrl+C to cancel...")
        try:
            input()
        except KeyboardInterrupt:
            print("\nCancelled.")
            return
    else:
        print("⚠️  Truncating fact tables (--yes flag provided)...")
    
    clear_fact_tables(skip_confirm=skip_confirm)
    print("\n✅ Done! Now you can re-run transform to repopulate from external tables.")


if __name__ == "__main__":
    main()

