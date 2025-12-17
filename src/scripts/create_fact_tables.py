"""
Script để tạo fact tables cho bills feature trong BigQuery.
Chạy SQL schema để tạo các tables: fact_sales_bills_v3_0 và fact_sales_bills_product_v3_0
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from google.cloud import bigquery
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def create_fact_tables():
    """Tạo fact tables cho bills feature."""
    bq_client = bigquery.Client(project=settings.gcp_project)
    
    project_id = settings.gcp_project
    dataset = settings.target_dataset
    dataset_id = f"{project_id}.{dataset}"
    
    # Ensure dataset exists
    try:
        bq_client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        logger.info(f"Creating dataset {dataset_id}...")
        dataset_obj = bigquery.Dataset(dataset_id)
        dataset_obj.location = settings.gcp_region
        dataset_obj.description = "NhanhVN fact tables - Flattened sales data"
        bq_client.create_dataset(dataset_obj, exists_ok=True)
        logger.info(f"Created dataset {dataset_id}")
    
    # SQL for fact_sales_bills_v3_0
    bills_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.fact_sales_bills_v3_0` (
        id INT64,
        depotId INT64,
        date DATE,
        type INT64,
        mode INT64,
        
        -- Customer info (Flattened from "customer" object)
        customer_id INT64,
        customer_name STRING,
        customer_mobile STRING,
        customer_address STRING,
        
        -- Sale/Staff info
        sale_id INT64,
        sale_name STRING,
        created_id INT64,
        created_email STRING,
        
        -- Payment info (Flattened from "payment" object)
        payment_total_amount FLOAT64,
        payment_customer_amount FLOAT64,
        payment_discount FLOAT64,
        payment_points FLOAT64,
        
        -- Flattened payment methods
        payment_cash_amount FLOAT64,
        payment_transfer_amount FLOAT64,
        payment_transfer_account_id INT64,
        payment_credit_amount FLOAT64,
        
        description STRING,
        extraction_timestamp TIMESTAMP
    )
    PARTITION BY date
    CLUSTER BY depotId, type;
    """
    
    # SQL for fact_sales_bills_product_v3_0
    products_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.fact_sales_bills_product_v3_0` (
        bill_id INT64,
        
        -- Product info
        product_id INT64,
        product_code STRING,
        product_barcode STRING,
        product_name STRING,
        
        -- Transaction info
        quantity FLOAT64,
        price FLOAT64,
        discount FLOAT64,
        vat_percent INT64,
        vat_amount FLOAT64,
        amount FLOAT64,
        
        -- Metadata
        bill_date DATE,
        extraction_timestamp TIMESTAMP
    )
    PARTITION BY bill_date
    CLUSTER BY bill_id, product_id;
    """
    
    # Execute SQL
    try:
        logger.info(f"Creating table fact_sales_bills_v3_0...")
        query_job = bq_client.query(bills_table_sql)
        query_job.result()
        logger.info(f"Successfully created table fact_sales_bills_v3_0")
    except Exception as e:
        logger.error(f"Error creating fact_sales_bills_v3_0: {e}")
        raise
    
    try:
        logger.info(f"Creating table fact_sales_bills_product_v3_0...")
        query_job = bq_client.query(products_table_sql)
        query_job.result()
        logger.info(f"Successfully created table fact_sales_bills_product_v3_0")
    except Exception as e:
        logger.error(f"Error creating fact_sales_bills_product_v3_0: {e}")
        raise
    
    logger.info("All fact tables created successfully!")


def main():
    """Main function."""
    try:
        create_fact_tables()
        print("Successfully created all fact tables!")
        return 0
    except Exception as e:
        logger.error(f"Failed to create fact tables: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())

