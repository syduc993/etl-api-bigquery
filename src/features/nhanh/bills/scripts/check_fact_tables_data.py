"""
Script để kiểm tra dữ liệu trong các bảng fact tables cho các ngày cụ thể.
"""
from google.cloud import bigquery
from src.config import settings
from datetime import date
from typing import List, Dict


def check_table_data(
    client: bigquery.Client,
    table_name: str,
    date_column: str,
    dates: List[date],
    dataset: str = None
) -> Dict[str, int]:
    """
    Kiểm tra số lượng records trong table cho các ngày cụ thể.
    
    Args:
        client: BigQuery client
        table_name: Tên table (ví dụ: 'fact_sales_bills_v3_0')
        date_column: Tên cột date để filter (ví dụ: 'date' hoặc 'extraction_timestamp')
        dates: Danh sách các ngày cần kiểm tra
        dataset: Dataset name (mặc định lấy từ settings.target_dataset)
        
    Returns:
        Dict với key là date string và value là số lượng records
    """
    if dataset is None:
        dataset = settings.target_dataset
    
    project_dataset = f"{settings.gcp_project}.{dataset}"
    full_table_id = f"{project_dataset}.{table_name}"
    
    results = {}
    
    for check_date in dates:
        date_str = check_date.strftime('%Y-%m-%d')
        
        # Build query based on date column type
        if date_column == 'extraction_timestamp':
            # For extraction_timestamp, use DATE() function
            query = f"""
            SELECT COUNT(*) as record_count
            FROM `{full_table_id}`
            WHERE DATE({date_column}) = '{date_str}'
            """
        else:
            # For date column directly
            query = f"""
            SELECT COUNT(*) as record_count
            FROM `{full_table_id}`
            WHERE {date_column} = '{date_str}'
            """
        
        try:
            query_job = client.query(query)
            results_query = query_job.result()
            
            for row in results_query:
                count = row.record_count
                results[date_str] = count
                print(f"  {date_str}: {count:,} records")
                
        except Exception as e:
            print(f"  {date_str}: ERROR - {str(e)}")
            results[date_str] = -1
    
    return results


def main():
    """Main function để kiểm tra dữ liệu cho các ngày 28, 29, 30 tháng 11 năm 2025."""
    
    # Dates cần kiểm tra
    dates_to_check = [
        date(2025, 11, 28),
        date(2025, 11, 29),
        date(2025, 11, 30),
    ]
    
    # Initialize BigQuery client
    client = bigquery.Client(
        project=settings.gcp_project,
        location=settings.gcp_region
    )
    
    print("=" * 80)
    print(f"Kiểm tra dữ liệu trong Fact Tables")
    print(f"Project: {settings.gcp_project}")
    print(f"Dataset: {settings.target_dataset}")
    print(f"Dates: 2025-11-28, 2025-11-29, 2025-11-30")
    print("=" * 80)
    print()
    
    # Check fact_sales_bills_v3_0
    print("📊 Bảng: fact_sales_bills_v3_0")
    print("   (Partitioned by 'date' column)")
    bills_results = check_table_data(
        client=client,
        table_name="fact_sales_bills_v3_0",
        date_column="date",
        dates=dates_to_check
    )
    print()
    
    # Check fact_sales_bills_product_v3_0
    # This table doesn't have a date column, so we need to join with bills table
    print("📊 Bảng: fact_sales_bills_product_v3_0")
    print("   (Join với fact_sales_bills_v3_0 để lấy date)")
    
    products_results = {}
    for check_date in dates_to_check:
        date_str = check_date.strftime('%Y-%m-%d')
        project_dataset = f"{settings.gcp_project}.{settings.target_dataset}"
        
        # Join với bills table để lấy products theo date của bill
        query = f"""
        SELECT COUNT(DISTINCT p.bill_id) as bill_count, COUNT(*) as product_count
        FROM `{project_dataset}.fact_sales_bills_product_v3_0` p
        INNER JOIN `{project_dataset}.fact_sales_bills_v3_0` b
        ON p.bill_id = b.id
        WHERE b.date = '{date_str}'
        """
        
        try:
            query_job = client.query(query)
            results_query = query_job.result()
            
            for row in results_query:
                bill_count = row.bill_count
                product_count = row.product_count
                products_results[date_str] = product_count
                print(f"  {date_str}: {product_count:,} product records (từ {bill_count:,} bills)")
                
        except Exception as e:
            print(f"  {date_str}: ERROR - {str(e)}")
            products_results[date_str] = -1
    
    print()
    
    # Summary
    print("=" * 80)
    print("📋 TÓM TẮT:")
    print("=" * 80)
    
    all_dates_have_data = True
    for check_date in dates_to_check:
        date_str = check_date.strftime('%Y-%m-%d')
        bills_count = bills_results.get(date_str, 0)
        products_count = products_results.get(date_str, 0)
        
        has_data = bills_count > 0 and products_count > 0
        status = "✅ CÓ DỮ LIỆU" if has_data else "❌ CHƯA CÓ DỮ LIỆU"
        
        print(f"\n{date_str}: {status}")
        print(f"  - fact_sales_bills_v3_0: {bills_count:,} records")
        print(f"  - fact_sales_bills_product_v3_0: {products_count:,} records")
        
        if not has_data:
            all_dates_have_data = False
    
    print()
    print("=" * 80)
    if all_dates_have_data:
        print("✅ TẤT CẢ CÁC NGÀY ĐỀU CÓ DỮ LIỆU - Schedule đang hoạt động tốt!")
    else:
        print("⚠️  MỘT SỐ NGÀY CHƯA CÓ DỮ LIỆU - Cần kiểm tra schedule hoặc pipeline")
    print("=" * 80)


if __name__ == "__main__":
    import sys
    import os
    # Add project root to path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))
    
    main()

