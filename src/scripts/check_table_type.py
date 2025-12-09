"""
Script để kiểm tra loại table trong BigQuery (External vs Native).
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from google.cloud import bigquery
from src.config import settings

def check_table_type(project_id: str, dataset_id: str, table_id: str):
    """Kiểm tra loại table."""
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    try:
        table = client.get_table(table_ref)
        
        print(f"\n📊 Table: {project_id}.{dataset_id}.{table_id}")
        print(f"   Type: {table.table_type}")
        
        if table.table_type == "EXTERNAL":
            print("   ✅ External Table (đọc từ GCS)")
            if table.external_data_configuration:
                print(f"   Source Format: {table.external_data_configuration.source_format}")
                if table.external_data_configuration.source_uris:
                    print(f"   Source URI: {table.external_data_configuration.source_uris[0][:80]}...")
        elif table.table_type == "TABLE":
            print("   ✅ Native Table (lưu trong BigQuery)")
            print(f"   Size: {table.num_bytes:,} bytes ({table.num_bytes / 1024 / 1024:.2f} MB)")
            print(f"   Rows: {table.num_rows:,}")
            if table.time_partitioning:
                print(f"   Partitioned by: {table.time_partitioning.field}")
            if table.clustering_fields:
                print(f"   Clustered by: {', '.join(table.clustering_fields)}")
        elif table.table_type == "VIEW":
            print("   📊 View")
        
        return table.table_type
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def check_all_tables_in_dataset(project_id: str, dataset_id: str):
    """Kiểm tra tất cả tables trong dataset."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    
    try:
        tables = list(client.list_tables(dataset_ref))
        
        print(f"\n📁 Dataset: {project_id}.{dataset_id}")
        print(f"   Total Tables: {len(tables)}")
        
        external_count = 0
        native_count = 0
        
        for table in tables:
            table_obj = client.get_table(table.reference)
            if table_obj.table_type == "EXTERNAL":
                external_count += 1
            elif table_obj.table_type == "TABLE":
                native_count += 1
        
        print(f"   External Tables: {external_count}")
        print(f"   Native Tables: {native_count}")
        
        return {"external": external_count, "native": native_count}
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check BigQuery table type")
    parser.add_argument("--dataset", type=str, help="Dataset name (e.g., bronze, nhanhVN)")
    parser.add_argument("--table", type=str, help="Table name (e.g., nhanh_bills_raw)")
    parser.add_argument("--all", action="store_true", help="Check all tables in dataset")
    
    args = parser.parse_args()
    
    project = settings.gcp_project
    
    if args.all and args.dataset:
        check_all_tables_in_dataset(project, args.dataset)
    elif args.dataset and args.table:
        check_table_type(project, args.dataset, args.table)
    else:
        # Default: Check example tables
        print("Checking example tables...")
        check_table_type(project, "bronze", "nhanh_bills_raw")
        check_table_type(project, "nhanhVN", "fact_sales_bills_v3_0")

if __name__ == "__main__":
    main()

