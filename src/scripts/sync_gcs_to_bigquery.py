"""
Script để đồng bộ data từ GCS sang BigQuery.

Kiểm tra xem GCS có những partition nào mà BigQuery chưa có,
sau đó load data từ GCS sang BigQuery cho các partition đó.
"""
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from src.config import settings
from src.shared.logging import get_logger
from src.features.nhanh.bills.components.loader import BillLoader

logger = get_logger(__name__)


def list_gcs_parquet_files(bucket_name: str, prefix: str) -> List[str]:
    """
    List tất cả parquet files trong GCS với prefix.
    
    Args:
        bucket_name: Tên GCS bucket
        prefix: Prefix path (ví dụ: 'nhanh/bills/')
        
    Returns:
        List[str]: Danh sách GCS URIs của các parquet files
    """
    storage_client = storage.Client(project=settings.gcp_project)
    bucket = storage_client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=prefix)
    parquet_files = []
    
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            parquet_files.append(gcs_uri)
    
    logger.info(f"Found {len(parquet_files)} parquet files in gs://{bucket_name}/{prefix}")
    return parquet_files


def extract_partition_date_from_path(gcs_uri: str, entity_type: str) -> date:
    """
    Extract partition date từ GCS path.
    
    Path format: gs://bucket/nhanh/bills/year=2025/month=11/data_2025-11-01_...
    hoặc: gs://bucket/nhanh/bill_products/year=2025/month=11/data_2025-11-01_...
    
    Args:
        gcs_uri: GCS URI của parquet file
        entity_type: 'bills' hoặc 'bill_products'
        
    Returns:
        date: Partition date
    """
    # Extract date từ filename: data_2025-11-01_...
    match = re.search(r'data_(\d{4}-\d{2}-\d{2})_', gcs_uri)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    
    # Fallback: extract từ path year=2025/month=11/day=01
    match = re.search(r'year=(\d{4})/month=(\d{2})(?:/day=(\d{2}))?', gcs_uri)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3)) if match.group(3) else 1
        return date(year, month, day)
    
    raise ValueError(f"Could not extract date from path: {gcs_uri}")


def get_bigquery_partitions(
    client: bigquery.Client,
    table_id: str,
    partition_field: str
) -> Set[date]:
    """
    Get danh sách partition dates đã có trong BigQuery table.
    
    Args:
        client: BigQuery client
        table_id: Full table ID
        partition_field: Tên field partition ('date' hoặc 'extraction_timestamp')
        
    Returns:
        Set[date]: Set các partition dates đã có
    """
    try:
        table = client.get_table(table_id)
        if not table.time_partitioning:
            logger.warning(f"Table {table_id} does not have partitioning")
            return set()
        
        # Query để lấy danh sách partitions
        if partition_field == "date":
            # Bills table: partition by date field
            query = f"""
            SELECT DISTINCT date as partition_date
            FROM `{table_id}`
            WHERE date IS NOT NULL
            """
        else:
            # Products table: partition by DATE(extraction_timestamp)
            query = f"""
            SELECT DISTINCT DATE(extraction_timestamp) as partition_date
            FROM `{table_id}`
            WHERE extraction_timestamp IS NOT NULL
            """
        
        query_job = client.query(query)
        results = query_job.result()
        
        partitions = {row.partition_date for row in results}
        logger.info(f"Found {len(partitions)} partitions in {table_id}")
        return partitions
        
    except NotFound:
        logger.info(f"Table {table_id} does not exist")
        return set()
    except Exception as e:
        logger.error(f"Error getting partitions from {table_id}: {e}")
        return set()


def sync_partition(
    loader: BillLoader,
    gcs_uri: str,
    partition_date: date,
    entity_type: str
) -> bool:
    """
    Sync một partition từ GCS sang BigQuery.
    
    Args:
        loader: BillLoader instance
        gcs_uri: GCS URI của parquet file
        partition_date: Partition date
        entity_type: 'bills' hoặc 'bill_products'
        
    Returns:
        bool: True nếu sync thành công
    """
    try:
        logger.info(f"Syncing {entity_type} partition {partition_date} from {gcs_uri}")
        
        if entity_type == "bills":
            loader.load_bills_from_gcs(gcs_uri, partition_date)
        else:
            loader.load_products_from_gcs(gcs_uri, partition_date)
        
        logger.info(f"Successfully synced {entity_type} partition {partition_date}")
        return True
        
    except Exception as e:
        logger.error(
            f"Failed to sync {entity_type} partition {partition_date}: {e}",
            gcs_uri=gcs_uri,
            error=str(e)
        )
        return False


def main():
    """Main function để sync data từ GCS sang BigQuery."""
    bq_client = bigquery.Client(project=settings.gcp_project, location=settings.gcp_region)
    loader = BillLoader()
    
    dataset_id = f"{settings.gcp_project}.{settings.target_dataset}"
    bills_table_id = f"{dataset_id}.fact_sales_bills_v3_0"
    products_table_id = f"{dataset_id}.fact_sales_bills_product_v3_0"
    
    # Entities để sync
    entities = [
        {
            "type": "bills",
            "gcs_prefix": "nhanh/bills/",
            "table_id": bills_table_id,
            "partition_field": "date"
        },
        {
            "type": "bill_products",
            "gcs_prefix": "nhanh/bill_products/",
            "table_id": products_table_id,
            "partition_field": "extraction_timestamp"
        }
    ]
    
    total_synced = 0
    total_failed = 0
    
    for entity in entities:
        entity_type = entity["type"]
        gcs_prefix = entity["gcs_prefix"]
        table_id = entity["table_id"]
        partition_field = entity["partition_field"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {entity_type}")
        logger.info(f"{'='*60}")
        
        # 1. List parquet files trong GCS
        logger.info(f"Listing parquet files in gs://{settings.bronze_bucket}/{gcs_prefix}")
        gcs_files = list_gcs_parquet_files(settings.bronze_bucket, gcs_prefix)
        
        if not gcs_files:
            logger.info(f"No parquet files found for {entity_type}")
            continue
        
        # 2. Extract partition dates từ GCS files
        gcs_partitions: Dict[date, str] = {}
        for gcs_uri in gcs_files:
            try:
                partition_date = extract_partition_date_from_path(gcs_uri, entity_type)
                # Nếu có nhiều files cho cùng partition, giữ file mới nhất (theo tên)
                if partition_date not in gcs_partitions or gcs_uri > gcs_partitions[partition_date]:
                    gcs_partitions[partition_date] = gcs_uri
            except Exception as e:
                logger.warning(f"Could not extract date from {gcs_uri}: {e}")
                continue
        
        logger.info(f"Found {len(gcs_partitions)} unique partitions in GCS for {entity_type}")
        
        # 3. Get partitions đã có trong BigQuery
        bq_partitions = get_bigquery_partitions(bq_client, table_id, partition_field)
        
        # 4. Tìm partitions cần sync (có trong GCS nhưng chưa có trong BigQuery)
        partitions_to_sync = set(gcs_partitions.keys()) - bq_partitions
        
        if not partitions_to_sync:
            logger.info(f"All {len(gcs_partitions)} partitions already exist in BigQuery for {entity_type}")
            continue
        
        logger.info(f"Found {len(partitions_to_sync)} partitions to sync for {entity_type}")
        logger.info(f"Partitions to sync: {sorted(partitions_to_sync)}")
        
        # 5. Sync từng partition
        for partition_date in sorted(partitions_to_sync):
            gcs_uri = gcs_partitions[partition_date]
            success = sync_partition(loader, gcs_uri, partition_date, entity_type)
            
            if success:
                total_synced += 1
            else:
                total_failed += 1
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("Sync Summary")
    logger.info(f"{'='*60}")
    logger.info(f"Total partitions synced: {total_synced}")
    logger.info(f"Total partitions failed: {total_failed}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()

