"""
Script để kiểm tra duplicate records trong BigQuery fact tables.
Check duplicate dựa trên primary key của mỗi table.
"""
from google.cloud import bigquery
from src.config import settings
from datetime import date, timedelta
from typing import Dict, List, Optional


def check_duplicates_bills(
    client: bigquery.Client,
    table_name: str = "fact_sales_bills_v3_0",
    check_date: Optional[date] = None,
    dataset: str = None
) -> Dict:
    """
    Kiểm tra duplicate trong bảng bills dựa trên id (primary key).
    
    Args:
        client: BigQuery client
        table_name: Tên table
        check_date: Ngày cần check (None = check tất cả)
        dataset: Dataset name
        
    Returns:
        Dict với thông tin duplicate
    """
    if dataset is None:
        dataset = settings.target_dataset
    
    project_dataset = f"{settings.gcp_project}.{dataset}"
    full_table_id = f"{project_dataset}.{table_name}"
    
    # Build query
    if check_date:
        date_str = check_date.strftime('%Y-%m-%d')
        where_clause = f"WHERE date = '{date_str}'"
        date_info = f"for date {date_str}"
    else:
        where_clause = ""
        date_info = "across all dates"
    
    query = f"""
    WITH duplicate_check AS (
        SELECT 
            id,
            COUNT(*) as duplicate_count,
            ARRAY_AGG(STRUCT(
                date,
                depotId,
                extraction_timestamp
            ) ORDER BY extraction_timestamp DESC) as duplicate_records
        FROM `{full_table_id}`
        {where_clause}
        GROUP BY id
        HAVING COUNT(*) > 1
    )
    SELECT 
        COUNT(*) as total_duplicate_ids,
        SUM(duplicate_count) as total_duplicate_rows,
        SUM(duplicate_count - 1) as excess_rows
    FROM duplicate_check
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            return {
                "table": table_name,
                "date": check_date.isoformat() if check_date else "all",
                "has_duplicates": row.total_duplicate_ids > 0,
                "duplicate_ids_count": row.total_duplicate_ids,
                "total_duplicate_rows": row.total_duplicate_rows,
                "excess_rows": row.excess_rows
            }
        
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "has_duplicates": False,
            "duplicate_ids_count": 0,
            "total_duplicate_rows": 0,
            "excess_rows": 0
        }
        
    except Exception as e:
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "error": str(e)
        }


def check_duplicates_products(
    client: bigquery.Client,
    table_name: str = "fact_sales_bills_product_v3_0",
    check_date: Optional[date] = None,
    dataset: str = None
) -> Dict:
    """
    Kiểm tra duplicate trong bảng products dựa trên bill_id + product_id (composite key).
    
    Args:
        client: BigQuery client
        table_name: Tên table
        check_date: Ngày cần check (None = check tất cả)
        dataset: Dataset name
        
    Returns:
        Dict với thông tin duplicate
    """
    if dataset is None:
        dataset = settings.target_dataset
    
    project_dataset = f"{settings.gcp_project}.{dataset}"
    full_table_id = f"{project_dataset}.{table_name}"
    
    # Build query
    if check_date:
        date_str = check_date.strftime('%Y-%m-%d')
        where_clause = f"WHERE bill_date = '{date_str}'"
        date_info = f"for date {date_str}"
    else:
        where_clause = ""
        date_info = "across all dates"
    
    query = f"""
    WITH duplicate_check AS (
        SELECT 
            bill_id,
            product_id,
            COUNT(*) as duplicate_count,
            ARRAY_AGG(STRUCT(
                bill_date,
                extraction_timestamp
            ) ORDER BY extraction_timestamp DESC) as duplicate_records
        FROM `{full_table_id}`
        {where_clause}
        GROUP BY bill_id, product_id
        HAVING COUNT(*) > 1
    )
    SELECT 
        COUNT(*) as total_duplicate_keys,
        SUM(duplicate_count) as total_duplicate_rows,
        SUM(duplicate_count - 1) as excess_rows
    FROM duplicate_check
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            return {
                "table": table_name,
                "date": check_date.isoformat() if check_date else "all",
                "has_duplicates": row.total_duplicate_keys > 0,
                "duplicate_keys_count": row.total_duplicate_keys,
                "total_duplicate_rows": row.total_duplicate_rows,
                "excess_rows": row.excess_rows
            }
        
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "has_duplicates": False,
            "duplicate_keys_count": 0,
            "total_duplicate_rows": 0,
            "excess_rows": 0
        }
        
    except Exception as e:
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "error": str(e)
        }


def main():
    """Main function để kiểm tra duplicate."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Check for duplicates in BigQuery fact tables")
    parser.add_argument(
        "--date",
        type=str,
        help="Date to check (YYYY-MM-DD). If not provided, checks all dates."
    )
    parser.add_argument(
        "--table",
        type=str,
        choices=["bills", "products", "all"],
        default="all",
        help="Table to check: bills, products, or all (default: all)"
    )
    
    args = parser.parse_args()
    
    # Parse date if provided
    check_date = None
    if args.date:
        try:
            check_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Initialize BigQuery client
    client = bigquery.Client(
        project=settings.gcp_project,
        location=settings.gcp_region
    )
    
    print("=" * 80)
    print("KIEM TRA DUPLICATE TRONG BIGQUERY FACT TABLES")
    print("=" * 80)
    print(f"Project: {settings.gcp_project}")
    print(f"Dataset: {settings.target_dataset}")
    if check_date:
        print(f"Date: {check_date.isoformat()}")
    else:
        print("Date: All dates")
    print("=" * 80)
    print()
    
    results = []
    
    # Check bills table
    if args.table in ["bills", "all"]:
        print("[BILLS] Checking fact_sales_bills_v3_0 (primary key: id)...")
        bills_result = check_duplicates_bills(client, check_date=check_date)
        results.append(bills_result)
        
        if "error" in bills_result:
            print(f"  [ERROR] {bills_result['error']}")
        elif bills_result["has_duplicates"]:
            print(f"  [WARNING] FOUND DUPLICATES:")
            print(f"     - Duplicate IDs: {bills_result['duplicate_ids_count']:,}")
            print(f"     - Total duplicate rows: {bills_result['total_duplicate_rows']:,}")
            print(f"     - Excess rows (should be deleted): {bills_result['excess_rows']:,}")
        else:
            print(f"  [OK] NO DUPLICATES FOUND")
        print()
    
    # Check products table
    if args.table in ["products", "all"]:
        print("[PRODUCTS] Checking fact_sales_bills_product_v3_0 (primary key: bill_id + product_id)...")
        products_result = check_duplicates_products(client, check_date=check_date)
        results.append(products_result)
        
        if "error" in products_result:
            print(f"  [ERROR] {products_result['error']}")
        elif products_result["has_duplicates"]:
            print(f"  [WARNING] FOUND DUPLICATES:")
            print(f"     - Duplicate keys: {products_result['duplicate_keys_count']:,}")
            print(f"     - Total duplicate rows: {products_result['total_duplicate_rows']:,}")
            print(f"     - Excess rows (should be deleted): {products_result['excess_rows']:,}")
        else:
            print(f"  [OK] NO DUPLICATES FOUND")
        print()
    
    # Summary
    print("=" * 80)
    print("TOM TAT:")
    print("=" * 80)
    
    has_any_duplicates = any(r.get("has_duplicates", False) for r in results if "error" not in r)
    has_errors = any("error" in r for r in results)
    
    if has_errors:
        print("[ERROR] CO LOI KHI KIEM TRA")
    elif has_any_duplicates:
        print("[WARNING] PHAT HIEN DUPLICATE - Can xu ly!")
        print("\nGoi y:")
        print("   - Re-run pipeline voi fix moi se tu dong xoa duplicate")
        print("   - Hoac xoa duplicate thu cong bang SQL")
    else:
        print("[OK] KHONG CO DUPLICATE - Du lieu sach!")
    
    print("=" * 80)


if __name__ == "__main__":
    main()

