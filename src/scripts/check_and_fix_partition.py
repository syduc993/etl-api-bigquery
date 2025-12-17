"""
Script để check và fix partition cho fact tables trong BigQuery.

Kiểm tra xem table có time partitioning hay không.
Nếu không có partition, sẽ tạo lại table với schema SQL đúng.
"""
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def load_schema_sql() -> str:
    """
    Load schema SQL từ file schema_clean.sql.
    
    Returns:
        str: Nội dung SQL schema
    """
    # Get project root (script is at src/scripts/, need to go up 2 levels)
    project_root = Path(__file__).parent.parent.parent
    schema_file = project_root / "src" / "features" / "nhanh" / "bills" / "sql" / "schema_clean.sql"
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    with open(schema_file, 'r', encoding='utf-8') as f:
        return f.read()


def format_schema_sql(sql_template: str, project_id: str, dataset: str) -> str:
    """
    Format SQL template với project_id và dataset.
    
    Args:
        sql_template: SQL template với {project_id} và {dataset}
        project_id: GCP project ID
        dataset: BigQuery dataset name
        
    Returns:
        str: Formatted SQL
    """
    return sql_template.format(project_id=project_id, dataset=dataset)


def check_table_partition(client: bigquery.Client, table_id: str) -> tuple[bool, bool]:
    """
    Check xem table có tồn tại và có partition hay không.
    
    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        
    Returns:
        tuple: (table_exists, has_partitioning)
    """
    try:
        table = client.get_table(table_id)
        has_partitioning = table.time_partitioning is not None
        
        if has_partitioning:
            partition_type = table.time_partitioning.type_
            partition_field = table.time_partitioning.field
            logger.info(
                f"Table {table_id} has partitioning",
                partition_type=partition_type,
                partition_field=partition_field
            )
        else:
            logger.warning(f"Table {table_id} exists but has NO partitioning")
        
        return True, has_partitioning
        
    except NotFound:
        logger.info(f"Table {table_id} does not exist")
        return False, False
    except Exception as e:
        logger.error(f"Error checking table {table_id}: {e}")
        raise


def extract_table_sql(sql_content: str, table_name: str) -> str:
    """
    Extract SQL statement cho một table cụ thể từ schema SQL.
    
    Args:
        sql_content: Full SQL content từ schema file
        table_name: Tên table (fact_sales_bills_v3_0 hoặc fact_sales_bills_product_v3_0)
        
    Returns:
        str: SQL statement cho table đó
    """
    lines = sql_content.split('\n')
    
    start_idx = None
    end_idx = None
    in_table = False
    
    for i, line in enumerate(lines):
        # Tìm dòng bắt đầu CREATE TABLE cho table này
        if "CREATE OR REPLACE TABLE" in line and table_name in line:
            start_idx = i
            in_table = True
        elif in_table and start_idx is not None:
            # Tìm dòng kết thúc (có ;)
            if line.strip().endswith(';'):
                end_idx = i + 1
                break
    
    if start_idx is None:
        raise ValueError(f"Could not find CREATE TABLE statement for {table_name}")
    
    if end_idx is None:
        # Fallback: lấy đến cuối file
        end_idx = len(lines)
    
    # Extract SQL statement
    table_sql = '\n'.join(lines[start_idx:end_idx])
    
    # Verify table name matches
    if table_name not in table_sql:
        raise ValueError(f"Table name mismatch: expected {table_name} in SQL")
    
    return table_sql


def create_table_with_partition(
    client: bigquery.Client,
    sql: str,
    table_id: str
) -> None:
    """
    Execute SQL để tạo table với partition.
    
    Args:
        client: BigQuery client
        sql: SQL statement để tạo table
        table_id: Full table ID (for logging)
    """
    try:
        logger.info(f"Creating table with partition: {table_id}")
        query_job = client.query(sql)
        query_job.result()  # Wait for completion
        
        logger.info(f"Successfully created table with partition: {table_id}")
        
        # Verify partition was created
        table = client.get_table(table_id)
        if table.time_partitioning:
            logger.info(
                f"Verified partition created",
                partition_type=table.time_partitioning.type_,
                partition_field=table.time_partitioning.field
            )
        else:
            logger.warning(f"Table created but partition not found: {table_id}")
            
    except Exception as e:
        logger.error(f"Failed to create table {table_id}: {e}")
        raise


def main():
    """Main function để check và fix partition cho cả 2 tables."""
    client = bigquery.Client(project=settings.gcp_project, location=settings.gcp_region)
    dataset_id = f"{settings.gcp_project}.{settings.target_dataset}"
    
    tables_to_check = [
        f"{dataset_id}.fact_sales_bills_v3_0",
        f"{dataset_id}.fact_sales_bills_product_v3_0"
    ]
    
    # Load schema SQL
    logger.info("Loading schema SQL...")
    schema_sql = load_schema_sql()
    formatted_sql = format_schema_sql(schema_sql, settings.gcp_project, settings.target_dataset)
    
    # Check và fix từng table
    for table_id in tables_to_check:
        table_name = table_id.split('.')[-1]
        logger.info(f"\n{'='*60}")
        logger.info(f"Checking table: {table_id}")
        logger.info(f"{'='*60}")
        
        # Check partition
        table_exists, has_partitioning = check_table_partition(client, table_id)
        
        if not table_exists:
            logger.info(f"Table {table_id} does not exist. Will create with partition.")
            # Extract SQL for this table
            table_sql = extract_table_sql(formatted_sql, table_name)
            create_table_with_partition(client, table_sql, table_id)
        elif not has_partitioning:
            logger.warning(f"Table {table_id} exists but has NO partitioning. Recreating with partition...")
            # Extract SQL for this table
            table_sql = extract_table_sql(formatted_sql, table_name)
            # Note: CREATE OR REPLACE will drop existing data
            logger.warning(f"WARNING: This will DROP existing data in {table_id}")
            create_table_with_partition(client, table_sql, table_id)
        else:
            logger.info(f"Table {table_id} already has partitioning. No action needed.")
    
    logger.info(f"\n{'='*60}")
    logger.info("Check and fix partition completed!")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()

