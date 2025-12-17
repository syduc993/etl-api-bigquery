"""
Script để xóa tất cả data của nhanh/bills trong GCS và BigQuery.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def clear_gcs_bills_data(bucket_name: str):
    """Xóa tất cả files trong GCS buckets liên quan đến nhanh/bills."""
    storage_client = storage.Client(project=settings.gcp_project)
    
    try:
        bucket = storage_client.bucket(bucket_name)
        
        if not bucket.exists():
            logger.info(f"Bucket {bucket_name} does not exist, skipping...")
            return 0
        
        # Prefixes cần xóa
        prefixes = ["nhanh/bills/", "nhanh/bill_products/"]
        total_deleted = 0
        
        for prefix in prefixes:
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                logger.info(f"No files found with prefix '{prefix}' in bucket {bucket_name}")
                continue
            
            logger.info(f"Found {len(blobs)} files with prefix '{prefix}' in bucket {bucket_name}")
            
            # Delete in batches
            batch_size = 1000
            for i in range(0, len(blobs), batch_size):
                batch = blobs[i:i + batch_size]
                bucket.delete_blobs(batch)
                total_deleted += len(batch)
                logger.info(f"Deleted {total_deleted}/{len(blobs)} files from prefix '{prefix}'...")
            
            logger.info(f"Successfully deleted {len(blobs)} files from prefix '{prefix}'")
        
        return total_deleted
        
    except Exception as e:
        logger.error(f"Error deleting files from bucket {bucket_name}: {e}")
        raise


def clear_bigquery_bills_tables():
    """Xóa tất cả tables trong BigQuery liên quan đến nhanh/bills."""
    bq_client = bigquery.Client(project=settings.gcp_project)
    total_deleted = 0
    
    # Fact tables trong target_dataset
    fact_tables = [
        f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0",
        f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0"
    ]
    
    for table_id in fact_tables:
        try:
            # Delete all data using TRUNCATE instead of deleting table
            sql = f"TRUNCATE TABLE `{table_id}`"
            query_job = bq_client.query(sql)
            query_job.result()
            logger.info(f"Truncated table: {table_id}")
            total_deleted += 1
        except NotFound:
            logger.info(f"Table {table_id} does not exist, skipping...")
        except Exception as e:
            logger.error(f"Error truncating table {table_id}: {e}")
            raise
    
    # External tables trong bronze_dataset (nếu có)
    external_tables = [
        f"{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bills_raw",
        f"{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bill_products_raw"
    ]
    
    for table_id in external_tables:
        try:
            bq_client.delete_table(table_id, not_found_ok=True)
            logger.info(f"Deleted external table: {table_id}")
            total_deleted += 1
        except Exception as e:
            logger.debug(f"External table {table_id} does not exist or error: {e}")
    
    return total_deleted


def main(confirm: bool = True):
    """Main function để xóa tất cả data của nhanh/bills."""
    project_id = settings.gcp_project
    
    print("=" * 80)
    print("WARNING: This will delete ALL nhanh/bills data in GCS and BigQuery!")
    print("=" * 80)
    print(f"\nProject: {project_id}")
    print(f"\nGCS Buckets to clear:")
    print(f"  - {settings.bronze_bucket} (prefix: nhanh/bills/, nhanh/bill_products/)")
    print(f"\nBigQuery Tables to clear:")
    print(f"  - {project_id}.{settings.target_dataset}.fact_sales_bills_v3_0 (TRUNCATE)")
    print(f"  - {project_id}.{settings.target_dataset}.fact_sales_bills_product_v3_0 (TRUNCATE)")
    print(f"  - {project_id}.{settings.bronze_dataset}.nhanh_bills_raw (if exists)")
    print(f"  - {project_id}.{settings.bronze_dataset}.nhanh_bill_products_raw (if exists)")
    print("\n" + "=" * 80)
    
    if confirm:
        print("Press Enter to continue or Ctrl+C to cancel...")
        print("=" * 80)
        try:
            input()
        except (KeyboardInterrupt, EOFError):
            print("\nCancelled.")
            return
    else:
        print("Running without confirmation...")
        print("=" * 80)
    
    total_files_deleted = 0
    total_tables_deleted = 0
    
    # 1. Xóa data trong GCS buckets
    print("\n" + "=" * 80)
    print("Clearing GCS Bills Data...")
    print("=" * 80)
    
    try:
        count = clear_gcs_bills_data(settings.bronze_bucket)
        total_files_deleted += count
    except Exception as e:
        logger.error(f"Failed to clear GCS data: {e}")
    
    # 2. Xóa data trong BigQuery tables
    print("\n" + "=" * 80)
    print("Clearing BigQuery Bills Tables...")
    print("=" * 80)
    
    try:
        count = clear_bigquery_bills_tables()
        total_tables_deleted += count
    except Exception as e:
        logger.error(f"Failed to clear BigQuery tables: {e}")
    
    # Summary
    print("\n" + "=" * 80)
    print("CLEANUP COMPLETE")
    print("=" * 80)
    print(f"Total files deleted from GCS: {total_files_deleted}")
    print(f"Total tables cleared from BigQuery: {total_tables_deleted}")
    print("=" * 80)


if __name__ == "__main__":
    import sys
    skip_confirm = "--yes" in sys.argv or "-y" in sys.argv
    main(confirm=not skip_confirm)

