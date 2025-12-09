from google.cloud import bigquery, storage
from src.config import settings

def main():
    project_id = settings.gcp_project
    dataset_id = f"{project_id}.{settings.bronze_dataset}"
    bucket_name = settings.bronze_bucket
    
    print(f"Cleaning up resources for project: {project_id}")
    
    # 1. Delete Dataset
    bq_client = bigquery.Client(project=project_id)
    try:
        print(f"Deleting dataset {dataset_id}...")
        bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print("Dataset deleted.")
    except Exception as e:
        print(f"Error deleting dataset: {e}")
        
    # 2. Delete Bucket
    storage_client = storage.Client(project=project_id)
    try:
        bucket = storage_client.bucket(bucket_name)
        if bucket.exists():
            print(f"Deleting bucket {bucket_name}...")
            # Delete all blobs first
            blobs = list(bucket.list_blobs())
            if blobs:
                bucket.delete_blobs(blobs)
                print(f"  Deleted {len(blobs)} files.")
            
            bucket.delete(force=True)
            print("Bucket deleted.")
        else:
            print("Bucket does not exist.")
    except Exception as e:
        print(f"Error deleting bucket: {e}")

if __name__ == "__main__":
    main()
