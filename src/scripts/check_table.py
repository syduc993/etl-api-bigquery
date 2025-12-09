from google.cloud import bigquery
from src.config import settings
from google.api_core.exceptions import NotFound

def main():
    client = bigquery.Client(project=settings.gcp_project, location=settings.gcp_region)
    dataset_id = f"{settings.gcp_project}.{settings.target_dataset}"
    table_id = f"{dataset_id}.fact_sales_bills_v3_0"
    
    print(f"Checking table: {table_id}")
    try:
        table = client.get_table(table_id)
        print(f"Table exists. Created: {table.created}")
    except NotFound:
        print("Table does not exist.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
