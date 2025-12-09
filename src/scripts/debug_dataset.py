from google.cloud import bigquery
from src.config import settings

def main():
    client = bigquery.Client(project=settings.gcp_project)
    
    datasets = [settings.bronze_dataset, settings.target_dataset]
    
    for dataset_id in datasets:
        full_dataset_id = f"{settings.gcp_project}.{dataset_id}"
        print(f"Checking dataset: {full_dataset_id}")
        try:
            dataset = client.get_dataset(full_dataset_id)
            print(f"  Location: {dataset.location}")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    main()
