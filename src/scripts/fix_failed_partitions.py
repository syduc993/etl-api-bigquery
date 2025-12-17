"""
Script để fix các partitions bị lỗi:
1. Re-extract bills partitions bị lỗi do timestamp nanoseconds
2. Fix schema mismatch cho bill_products partition 2025-12-12
"""
from datetime import datetime, date
from src.features.nhanh.bills.pipeline import BillPipeline
from src.features.nhanh.bills.components.loader import BillLoader
from src.shared.logging import get_logger
from google.cloud import bigquery
from src.config import settings

logger = get_logger(__name__)


def re_extract_bills_partitions(failed_dates: list[date]):
    """
    Re-extract bills partitions bị lỗi do timestamp nanoseconds.
    
    Args:
        failed_dates: List các dates cần re-extract
    """
    logger.info(f"Re-extracting {len(failed_dates)} bills partitions...")
    
    pipeline = BillPipeline()
    
    # Group dates thành ranges để extract hiệu quả hơn
    sorted_dates = sorted(failed_dates)
    from_date = datetime.combine(sorted_dates[0], datetime.min.time())
    to_date = datetime.combine(sorted_dates[-1], datetime.max.time())
    
    logger.info(f"Extracting bills from {from_date.date()} to {to_date.date()}")
    
    try:
        result = pipeline.run_extract_load(
            from_date=from_date,
            to_date=to_date,
            process_by_day=True
        )
        
        logger.info(f"Re-extraction completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Re-extraction failed: {e}")
        raise


def fix_bill_products_schema_mismatch(partition_date: date):
    """
    Fix schema mismatch cho bill_products partition.
    
    Vấn đề: extendedWarrantyAmount có type conflict (FLOAT vs INTEGER)
    Giải pháp: Load lại file với schema update options
    """
    logger.info(f"Fixing schema mismatch for bill_products partition {partition_date}")
    
    loader = BillLoader()
    bq_client = bigquery.Client(project=settings.gcp_project, location=settings.gcp_region)
    
    # Tìm file GCS cho partition này
    from google.cloud import storage
    storage_client = storage.Client(project=settings.gcp_project)
    bucket = storage_client.bucket(settings.bronze_bucket)
    
    # Tìm file parquet cho partition date
    prefix = f"nhanh/bill_products/year={partition_date.year}/month={partition_date.month:02d}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    # Tìm file có partition date trong tên
    target_file = None
    for blob in blobs:
        if blob.name.endswith('.parquet') and partition_date.isoformat() in blob.name:
            target_file = f"gs://{settings.bronze_bucket}/{blob.name}"
            break
    
    if not target_file:
        logger.error(f"Could not find GCS file for partition {partition_date}")
        return False
    
    logger.info(f"Found GCS file: {target_file}")
    
    # Delete partition data cũ
    table_id = f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0"
    
    try:
        # Delete partition
        sql = f"""
        DELETE FROM `{table_id}`
        WHERE DATE(extraction_timestamp) = DATE('{partition_date.isoformat()}')
        """
        query_job = bq_client.query(sql)
        query_job.result()
        logger.info(f"Deleted old data for partition {partition_date}")
    except Exception as e:
        logger.warning(f"Could not delete old data: {e}")
    
    # Load lại với schema update options
    try:
        loader.load_products_from_gcs(target_file, partition_date)
        logger.info(f"Successfully reloaded partition {partition_date}")
        return True
    except Exception as e:
        logger.error(f"Failed to reload partition {partition_date}: {e}")
        return False


def main():
    """Main function để fix các partitions bị lỗi."""
    
    # 1. Re-extract bills partitions bị lỗi
    failed_bills_dates = [
        date(2025, 11, 1), date(2025, 11, 2), date(2025, 11, 3), date(2025, 11, 4),
        date(2025, 11, 5), date(2025, 11, 6), date(2025, 11, 7), date(2025, 11, 8),
        date(2025, 11, 9), date(2025, 11, 10), date(2025, 11, 11), date(2025, 11, 12),
        date(2025, 11, 13), date(2025, 11, 14), date(2025, 11, 15), date(2025, 11, 16),
        date(2025, 11, 27), date(2025, 12, 11), date(2025, 12, 15), date(2025, 12, 16)
    ]
    
    logger.info(f"\n{'='*60}")
    logger.info("Step 1: Re-extracting failed bills partitions")
    logger.info(f"{'='*60}")
    
    try:
        re_extract_bills_partitions(failed_bills_dates)
        logger.info("✅ Bills re-extraction completed successfully")
    except Exception as e:
        logger.error(f"❌ Bills re-extraction failed: {e}")
    
    # 2. Fix bill_products schema mismatch
    logger.info(f"\n{'='*60}")
    logger.info("Step 2: Fixing bill_products schema mismatch")
    logger.info(f"{'='*60}")
    
    try:
        success = fix_bill_products_schema_mismatch(date(2025, 12, 12))
        if success:
            logger.info("✅ Bill products schema fix completed successfully")
        else:
            logger.error("❌ Bill products schema fix failed")
    except Exception as e:
        logger.error(f"❌ Bill products schema fix failed: {e}")
    
    logger.info(f"\n{'='*60}")
    logger.info("Fix completed!")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()

