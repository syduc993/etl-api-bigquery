"""
Script để chẩn đoán tại sao dữ liệu không có trong fact tables cho các ngày cụ thể.
Kiểm tra toàn bộ pipeline: GCS -> Bronze External Tables -> Fact Tables
"""
import sys
import os
from datetime import date, datetime
from typing import Dict, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from google.cloud import bigquery
from google.cloud import storage
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def check_gcs_data(storage_client: storage.Client, dates: List[date]) -> Dict[str, Dict[str, int]]:
    """Kiểm tra dữ liệu trong GCS cho các ngày cụ thể."""
    bucket_name = settings.bronze_bucket
    bucket = storage_client.bucket(bucket_name)
    
    results = {}
    
    for check_date in dates:
        date_str = check_date.strftime('%Y-%m-%d')
        results[date_str] = {"bills": 0, "bill_products": 0}
        
        # Check bills - partition strategy có thể là "month" hoặc "day"
        # Thử cả 2 format
        bills_prefix_month = f"nhanh/bills/year={check_date.year}/month={check_date.month:02d}/"
        bills_prefix_day = f"nhanh/bills/year={check_date.year}/month={check_date.month:02d}/day={check_date.day:02d}/"
        
        bills_blobs_month = list(bucket.list_blobs(prefix=bills_prefix_month))
        bills_blobs_day = list(bucket.list_blobs(prefix=bills_prefix_day))
        
        # Filter blobs có chứa date trong filename
        date_pattern = check_date.isoformat()
        bills_count = sum(1 for blob in bills_blobs_month + bills_blobs_day 
                         if date_pattern in blob.name)
        results[date_str]["bills"] = bills_count
        
        # Check bill_products
        products_prefix_month = f"nhanh/bill_products/year={check_date.year}/month={check_date.month:02d}/"
        products_prefix_day = f"nhanh/bill_products/year={check_date.year}/month={check_date.month:02d}/day={check_date.day:02d}/"
        
        products_blobs_month = list(bucket.list_blobs(prefix=products_prefix_month))
        products_blobs_day = list(bucket.list_blobs(prefix=products_prefix_day))
        
        products_count = sum(1 for blob in products_blobs_month + products_blobs_day 
                            if date_pattern in blob.name)
        results[date_str]["bill_products"] = products_count
    
    return results


def check_bronze_external_tables(bq_client: bigquery.Client, dates: List[date]) -> Dict[str, Dict[str, int]]:
    """Kiểm tra dữ liệu trong Bronze External Tables cho các ngày cụ thể."""
    results = {}
    
    for check_date in dates:
        date_str = check_date.strftime('%Y-%m-%d')
        results[date_str] = {"bills": 0, "bill_products": 0}
        
        # Check bills external table
        bills_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bills_raw`
        WHERE PARSE_DATE('%Y-%m-%d', date) = '{date_str}'
        """
        try:
            bills_job = bq_client.query(bills_query)
            bills_result = list(bills_job.result())
            results[date_str]["bills"] = bills_result[0].cnt if bills_result else 0
        except Exception as e:
            logger.error(f"Error checking bronze bills for {date_str}: {e}")
            results[date_str]["bills"] = -1
        
        # Check bill_products external table (không có date column, cần join với bills)
        products_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bill_products_raw` p
        INNER JOIN `{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bills_raw` b
        ON p.bill_id = b.id
        WHERE PARSE_DATE('%Y-%m-%d', b.date) = '{date_str}'
        """
        try:
            products_job = bq_client.query(products_query)
            products_result = list(products_job.result())
            results[date_str]["bill_products"] = products_result[0].cnt if products_result else 0
        except Exception as e:
            logger.error(f"Error checking bronze products for {date_str}: {e}")
            results[date_str]["bill_products"] = -1
    
    return results


def check_fact_tables(bq_client: bigquery.Client, dates: List[date]) -> Dict[str, Dict[str, int]]:
    """Kiểm tra dữ liệu trong Fact Tables cho các ngày cụ thể."""
    results = {}
    
    for check_date in dates:
        date_str = check_date.strftime('%Y-%m-%d')
        results[date_str] = {"bills": 0, "bill_products": 0}
        
        # Check fact_sales_bills_v3_0
        bills_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0`
        WHERE date = '{date_str}'
        """
        try:
            bills_job = bq_client.query(bills_query)
            bills_result = list(bills_job.result())
            results[date_str]["bills"] = bills_result[0].cnt if bills_result else 0
        except Exception as e:
            logger.error(f"Error checking fact bills for {date_str}: {e}")
            results[date_str]["bills"] = -1
        
        # Check fact_sales_bills_product_v3_0
        products_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0` p
        INNER JOIN `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0` b
        ON p.bill_id = b.id
        WHERE b.date = '{date_str}'
        """
        try:
            products_job = bq_client.query(products_query)
            products_result = list(products_job.result())
            results[date_str]["bill_products"] = products_result[0].cnt if products_result else 0
        except Exception as e:
            logger.error(f"Error checking fact products for {date_str}: {e}")
            results[date_str]["bill_products"] = -1
    
    return results


def main():
    """Main function để chẩn đoán vấn đề."""
    
    dates_to_check = [
        date(2025, 11, 28),
        date(2025, 11, 29),
        date(2025, 11, 30),
    ]
    
    print("=" * 80)
    print("🔍 CHẨN ĐOÁN VẤN ĐỀ: Dữ liệu thiếu trong Fact Tables")
    print(f"Project: {settings.gcp_project}")
    print(f"Dates: 2025-11-28, 2025-11-29, 2025-11-30")
    print("=" * 80)
    print()
    
    # Initialize clients
    bq_client = bigquery.Client(
        project=settings.gcp_project,
        location=settings.gcp_region
    )
    storage_client = storage.Client(project=settings.gcp_project)
    
    # Step 1: Check GCS
    print("📦 BƯỚC 1: Kiểm tra dữ liệu trong GCS (Bronze Bucket)")
    print("-" * 80)
    gcs_results = check_gcs_data(storage_client, dates_to_check)
    for date_str in [d.strftime('%Y-%m-%d') for d in dates_to_check]:
        bills_files = gcs_results[date_str]["bills"]
        products_files = gcs_results[date_str]["bill_products"]
        status = "✅" if bills_files > 0 and products_files > 0 else "❌"
        print(f"{status} {date_str}:")
        print(f"   - Bills files: {bills_files}")
        print(f"   - Bill Products files: {products_files}")
    print()
    
    # Step 2: Check Bronze External Tables
    print("📊 BƯỚC 2: Kiểm tra dữ liệu trong Bronze External Tables")
    print("-" * 80)
    bronze_results = check_bronze_external_tables(bq_client, dates_to_check)
    for date_str in [d.strftime('%Y-%m-%d') for d in dates_to_check]:
        bills_count = bronze_results[date_str]["bills"]
        products_count = bronze_results[date_str]["bill_products"]
        status = "✅" if bills_count > 0 and products_count > 0 else "❌"
        print(f"{status} {date_str}:")
        print(f"   - nhanh_bills_raw: {bills_count:,} records")
        print(f"   - nhanh_bill_products_raw: {products_count:,} records")
    print()
    
    # Step 3: Check Fact Tables
    print("🎯 BƯỚC 3: Kiểm tra dữ liệu trong Fact Tables")
    print("-" * 80)
    fact_results = check_fact_tables(bq_client, dates_to_check)
    for date_str in [d.strftime('%Y-%m-%d') for d in dates_to_check]:
        bills_count = fact_results[date_str]["bills"]
        products_count = fact_results[date_str]["bill_products"]
        status = "✅" if bills_count > 0 and products_count > 0 else "❌"
        print(f"{status} {date_str}:")
        print(f"   - fact_sales_bills_v3_0: {bills_count:,} records")
        print(f"   - fact_sales_bills_product_v3_0: {products_count:,} records")
    print()
    
    # Diagnosis
    print("=" * 80)
    print("🔬 CHẨN ĐOÁN:")
    print("=" * 80)
    
    for check_date in dates_to_check:
        date_str = check_date.strftime('%Y-%m-%d')
        gcs_bills = gcs_results[date_str]["bills"]
        gcs_products = gcs_results[date_str]["bill_products"]
        bronze_bills = bronze_results[date_str]["bills"]
        bronze_products = bronze_results[date_str]["bill_products"]
        fact_bills = fact_results[date_str]["bills"]
        fact_products = fact_results[date_str]["bill_products"]
        
        print(f"\n📅 {date_str}:")
        
        if gcs_bills == 0 and gcs_products == 0:
            print("   ❌ VẤN ĐỀ: Không có dữ liệu trong GCS")
            print("      → Extract step chưa chạy hoặc failed")
            print("      → Cần kiểm tra schedule extract hoặc chạy manual extract")
        elif bronze_bills == 0 and bronze_products == 0:
            print("   ❌ VẤN ĐỀ: Có dữ liệu trong GCS nhưng không có trong External Tables")
            print("      → External Tables chưa được update sau khi upload GCS")
            print("      → Cần chạy: BigQueryExternalTableSetup.setup_all_tables()")
        elif fact_bills == 0 and fact_products == 0:
            print("   ❌ VẤN ĐỀ: Có dữ liệu trong Bronze nhưng không có trong Fact Tables")
            print("      → Transform step chưa chạy hoặc failed")
            print("      → Cần chạy: BillTransformer.transform_flatten()")
            print("      → Hoặc kiểm tra schedule transform")
        else:
            print("   ✅ Dữ liệu đầy đủ ở tất cả các layer")
    
    print()
    print("=" * 80)
    print("💡 GỢI Ý KHẮC PHỤC:")
    print("=" * 80)
    print("1. Nếu thiếu ở GCS: Chạy extract manual cho các ngày thiếu")
    print("2. Nếu thiếu ở Bronze External Tables: Chạy setup_external_tables()")
    print("3. Nếu thiếu ở Fact Tables: Chạy transform_flatten()")
    print("4. Kiểm tra schedule có đang chạy đúng không")
    print("=" * 80)


if __name__ == "__main__":
    main()

