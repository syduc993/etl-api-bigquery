"""
Script để xóa duplicate records trong BigQuery fact tables.
Giữ lại record mới nhất (extraction_timestamp cao nhất) và xóa các record cũ hơn.
"""
from google.cloud import bigquery
from src.config import settings
from datetime import date
from typing import Optional
import time


def remove_duplicates_bills(
    client: bigquery.Client,
    table_name: str = "fact_sales_bills_v3_0",
    check_date: Optional[date] = None,
    dataset: str = None,
    dry_run: bool = False
) -> dict:
    """
    Xóa duplicate trong bảng bills, giữ lại record mới nhất.
    
    Args:
        client: BigQuery client
        table_name: Tên table
        check_date: Ngày cần xóa (None = xóa tất cả)
        dataset: Dataset name
        dry_run: Nếu True, chỉ show query không chạy
        
    Returns:
        Dict với thông tin kết quả
    """
    if dataset is None:
        dataset = settings.target_dataset
    
    project_dataset = f"{settings.gcp_project}.{dataset}"
    full_table_id = f"{project_dataset}.{table_name}"
    
    # Build WHERE clause
    if check_date:
        date_str = check_date.strftime('%Y-%m-%d')
        where_clause = f"WHERE date = '{date_str}'"
    else:
        where_clause = ""
    
    # Query để xóa duplicate: giữ lại record có extraction_timestamp cao nhất
    # Sử dụng NOT EXISTS để xóa các record không phải là record mới nhất
    query = f"""
    DELETE FROM `{full_table_id}` t1
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT 
                id,
                extraction_timestamp,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY extraction_timestamp DESC) as rn
            FROM `{full_table_id}`
            {where_clause}
        ) t2
        WHERE t1.id = t2.id 
        AND t1.extraction_timestamp = t2.extraction_timestamp
        AND t2.rn > 1
    )
    """
    
    try:
        if dry_run:
            return {
                "table": table_name,
                "date": check_date.isoformat() if check_date else "all",
                "dry_run": True,
                "query": query
            }
        
        print(f"  Running DELETE query...")
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        deleted_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
        
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "deleted_rows": deleted_rows,
            "success": True
        }
        
    except Exception as e:
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "error": str(e),
            "success": False
        }


def remove_duplicates_products(
    client: bigquery.Client,
    table_name: str = "fact_sales_bills_product_v3_0",
    check_date: Optional[date] = None,
    dataset: str = None,
    dry_run: bool = False
) -> dict:
    """
    Xóa duplicate trong bảng products, giữ lại record mới nhất.
    
    Args:
        client: BigQuery client
        table_name: Tên table
        check_date: Ngày cần xóa (None = xóa tất cả)
        dataset: Dataset name
        dry_run: Nếu True, chỉ show query không chạy
        
    Returns:
        Dict với thông tin kết quả
    """
    if dataset is None:
        dataset = settings.target_dataset
    
    project_dataset = f"{settings.gcp_project}.{dataset}"
    full_table_id = f"{project_dataset}.{table_name}"
    
    # Build WHERE clause
    if check_date:
        date_str = check_date.strftime('%Y-%m-%d')
        where_clause = f"WHERE bill_date = '{date_str}'"
    else:
        where_clause = ""
    
    # Query để xóa duplicate: giữ lại record có extraction_timestamp cao nhất
    # Sử dụng NOT EXISTS để xóa các record không phải là record mới nhất
    query = f"""
    DELETE FROM `{full_table_id}` t1
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT 
                bill_id,
                product_id,
                extraction_timestamp,
                ROW_NUMBER() OVER (PARTITION BY bill_id, product_id ORDER BY extraction_timestamp DESC) as rn
            FROM `{full_table_id}`
            {where_clause}
        ) t2
        WHERE t1.bill_id = t2.bill_id 
        AND t1.product_id = t2.product_id
        AND t1.extraction_timestamp = t2.extraction_timestamp
        AND t2.rn > 1
    )
    """
    
    try:
        if dry_run:
            return {
                "table": table_name,
                "date": check_date.isoformat() if check_date else "all",
                "dry_run": True,
                "query": query
            }
        
        print(f"  Running DELETE query...")
        query_job = client.query(query)
        query_job.result()  # Wait for completion
        
        deleted_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
        
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "deleted_rows": deleted_rows,
            "success": True
        }
        
    except Exception as e:
        return {
            "table": table_name,
            "date": check_date.isoformat() if check_date else "all",
            "error": str(e),
            "success": False
        }


def main():
    """Main function để xóa duplicate."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Remove duplicates from BigQuery fact tables")
    parser.add_argument(
        "--date",
        type=str,
        help="Date to clean (YYYY-MM-DD). If not provided, cleans all dates."
    )
    parser.add_argument(
        "--table",
        type=str,
        choices=["bills", "products", "all"],
        default="all",
        help="Table to clean: bills, products, or all (default: all)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm deletion (required if not dry-run)"
    )
    
    args = parser.parse_args()
    
    # Parse date if provided
    check_date = None
    if args.date:
        try:
            check_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"[ERROR] Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Check confirmation
    if not args.dry_run and not args.confirm:
        print("[ERROR] --confirm flag is required for actual deletion")
        print("        Use --dry-run to preview what will be deleted")
        sys.exit(1)
    
    # Initialize BigQuery client
    client = bigquery.Client(
        project=settings.gcp_project,
        location=settings.gcp_region
    )
    
    print("=" * 80)
    if args.dry_run:
        print("DRY RUN - PREVIEW DELETE OPERATIONS")
    else:
        print("XOA DUPLICATE TRONG BIGQUERY FACT TABLES")
    print("=" * 80)
    print(f"Project: {settings.gcp_project}")
    print(f"Dataset: {settings.target_dataset}")
    if check_date:
        print(f"Date: {check_date.isoformat()}")
    else:
        print("Date: All dates")
    print("=" * 80)
    print()
    
    if not args.dry_run:
        print("[WARNING] This will DELETE duplicate records from BigQuery!")
        print("          Keeping only the latest record (highest extraction_timestamp)")
        print()
        response = input("Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("Cancelled.")
            sys.exit(0)
        print()
    
    results = []
    start_time = time.time()
    
    # Remove duplicates from bills table
    if args.table in ["bills", "all"]:
        print(f"[BILLS] Removing duplicates from fact_sales_bills_v3_0...")
        bills_result = remove_duplicates_bills(
            client, 
            check_date=check_date,
            dry_run=args.dry_run
        )
        results.append(bills_result)
        
        if args.dry_run:
            print(f"  [DRY RUN] Would execute DELETE query")
            print(f"  Query preview: {bills_result['query'][:200]}...")
        elif "error" in bills_result:
            print(f"  [ERROR] {bills_result['error']}")
        else:
            print(f"  [OK] Deleted {bills_result['deleted_rows']:,} duplicate rows")
        print()
    
    # Remove duplicates from products table
    if args.table in ["products", "all"]:
        print(f"[PRODUCTS] Removing duplicates from fact_sales_bills_product_v3_0...")
        products_result = remove_duplicates_products(
            client,
            check_date=check_date,
            dry_run=args.dry_run
        )
        results.append(products_result)
        
        if args.dry_run:
            print(f"  [DRY RUN] Would execute DELETE query")
            print(f"  Query preview: {products_result['query'][:200]}...")
        elif "error" in products_result:
            print(f"  [ERROR] {products_result['error']}")
        else:
            print(f"  [OK] Deleted {products_result['deleted_rows']:,} duplicate rows")
        print()
    
    elapsed_time = time.time() - start_time
    
    # Summary
    print("=" * 80)
    print("TOM TAT:")
    print("=" * 80)
    
    if args.dry_run:
        print("[DRY RUN] No records were actually deleted")
        print("          Run without --dry-run and with --confirm to execute")
    else:
        has_errors = any("error" in r for r in results)
        total_deleted = sum(r.get("deleted_rows", 0) for r in results if "error" not in r)
        
        if has_errors:
            print("[ERROR] Co loi khi xoa duplicate")
        else:
            print(f"[OK] Da xoa {total_deleted:,} duplicate rows")
            print(f"     Thoi gian: {elapsed_time:.2f} seconds")
    
    print("=" * 80)


if __name__ == "__main__":
    main()

