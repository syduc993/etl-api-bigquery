from google.cloud import bigquery
from src.config import settings

def main():
    client = bigquery.Client(project=settings.gcp_project)
    # External table is in bronze dataset
    table_id = f"{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bills_raw"
    
    query = f"SELECT COUNT(*) as count FROM `{table_id}`"
    print(f"Running query: {query}")
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            print(f"Record count: {row.count}")
    except Exception as e:
        print(f"Query failed: {e}")

if __name__ == "__main__":
    main()
