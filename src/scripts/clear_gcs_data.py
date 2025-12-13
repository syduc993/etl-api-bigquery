"""
Script để xóa tất cả data trong GCS bucket (chỉ xóa files, giữ bucket).
Dùng khi muốn xóa data cũ để tải lại với schema mới.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from google.cloud import storage
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def clear_bucket_data(bucket_name: str, prefix: str = None):
    """
    Xóa tất cả files trong bucket (hoặc trong prefix nếu có).
    
    Args:
        bucket_name: Tên GCS bucket
        prefix: Optional prefix để chỉ xóa files trong prefix đó (e.g., "nhanh/")
    """
    storage_client = storage.Client(project=settings.gcp_project)
    bucket = storage_client.bucket(bucket_name)
    
    if not bucket.exists():
        logger.warning(f"Bucket {bucket_name} does not exist")
        return 0
    
    # List và delete blobs
    if prefix:
        blobs = list(bucket.list_blobs(prefix=prefix))
        logger.info(f"Deleting files with prefix '{prefix}' in bucket {bucket_name}")
    else:
        blobs = list(bucket.list_blobs())
        logger.info(f"Deleting all files in bucket {bucket_name}")
    
    if not blobs:
        logger.info("No files to delete")
        return 0
    
    logger.info(f"Found {len(blobs)} files to delete")
    
    # Delete in batches (GCS allows up to 1000 at a time)
    deleted_count = 0
    batch_size = 1000
    
    for i in range(0, len(blobs), batch_size):
        batch = blobs[i:i + batch_size]
        bucket.delete_blobs(batch)
        deleted_count += len(batch)
        logger.info(f"Deleted {deleted_count}/{len(blobs)} files...")
    
    logger.info(f"✅ Successfully deleted {deleted_count} files from bucket {bucket_name}")
    return deleted_count


def main(confirm: bool = True):
    """Main function."""
    bucket_name = settings.bronze_bucket
    
    if confirm:
        print(f"⚠️  WARNING: This will delete all data in bucket: {bucket_name}")
        print("Press Enter to continue or Ctrl+C to cancel...")
        try:
            input()
        except KeyboardInterrupt:
            print("\nCancelled.")
            return
    else:
        print(f"⚠️  WARNING: Deleting all data in bucket: {bucket_name}")
    
    # Xóa tất cả data trong bucket
    deleted = clear_bucket_data(bucket_name)
    
    if deleted > 0:
        print(f"\n✅ Deleted {deleted} files successfully!")
    else:
        print("\n✅ No files found (bucket may be empty)")


if __name__ == "__main__":
    main()

