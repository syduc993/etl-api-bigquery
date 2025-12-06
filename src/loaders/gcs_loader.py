"""
GCS Loader với hỗ trợ partitioning.
File này xử lý việc upload data lên Google Cloud Storage với:
- Partitioning theo thời gian (month hoặc day level)
- Gzip compression để giảm storage cost
- Metadata tracking
- Idempotent uploads (không duplicate nếu file đã tồn tại)
"""
import json
import gzip
from datetime import datetime
from typing import Dict, Any, List, Optional
from google.cloud import storage
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


class GCSLoader:
    """
    Loader để upload data lên GCS với partitioning.
    
    Hỗ trợ 2 loại partitioning:
    - Month level: year={YYYY}/month={MM} (đơn giản hơn)
    - Day level: year={YYYY}/month={MM}/day={DD} (performance tốt hơn)
    """
    
    def __init__(self, bucket_name: str):
        """
        Khởi tạo GCS loader.
        
        Args:
            bucket_name: Tên GCS bucket để upload data
        """
        self.bucket_name = bucket_name
        self.storage_client = storage.Client(project=settings.gcp_project)
        self.bucket = self.storage_client.bucket(bucket_name)
    
    def _get_partition_path(
        self,
        entity: str,
        timestamp: datetime,
        partition_strategy: Optional[str] = None
    ) -> str:
        """
        Tạo partition path dựa trên strategy.
        
        Args:
            entity: Tên entity (ví dụ: 'bills', 'products', 'customers')
            timestamp: Timestamp để tạo partition
            partition_strategy: 'day' hoặc 'month' (mặc định lấy từ config)
            
        Returns:
            str: Partition path prefix (ví dụ: 'bills/year=2024/month=01/')
        """
        strategy = partition_strategy or settings.partition_strategy
        
        if strategy == "day":
            return (
                f"{entity}/"
                f"year={timestamp.year}/"
                f"month={timestamp.month:02d}/"
                f"day={timestamp.day:02d}/"
            )
        else:  # month
            return (
                f"{entity}/"
                f"year={timestamp.year}/"
                f"month={timestamp.month:02d}/"
            )
    
    def upload_json(
        self,
        entity: str,
        data: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
        compress: bool = True
    ) -> str:
        """
        Upload JSON data lên GCS với partitioning.
        
        Args:
            entity: Tên entity (ví dụ: 'bills', 'products', 'customers')
            data: Danh sách records để upload
            metadata: Metadata tùy chọn (request params, response status, etc.)
            compress: Có nén bằng gzip hay không (mặc định: True)
            
        Returns:
            str: GCS path của file đã upload (rỗng nếu không có data)
        """
        if not data:
            logger.warning(f"No data to upload for {entity}", entity=entity)
            return ""
        
        timestamp = datetime.utcnow()
        partition_path = self._get_partition_path(entity, timestamp)
        
        # Generate filename
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S_%f")
        filename = f"timestamp_{timestamp_str}.json"
        if compress:
            filename += ".gz"
        
        full_path = f"{partition_path}{filename}"
        
        # Check if file already exists (idempotency)
        blob = self.bucket.blob(full_path)
        if blob.exists():
            logger.warning(
                f"File already exists, skipping upload",
                path=full_path,
                entity=entity
            )
            return full_path
        
        # Prepare JSON content
        json_content = json.dumps(data, ensure_ascii=False, default=str)
        
        # Compress if requested
        if compress:
            content = gzip.compress(json_content.encode('utf-8'))
            content_type = 'application/gzip'
        else:
            content = json_content.encode('utf-8')
            content_type = 'application/json'
        
        # Upload to GCS
        blob.upload_from_string(
            content,
            content_type=content_type
        )
        
        logger.info(
            f"Uploaded {len(data)} records to GCS",
            path=full_path,
            entity=entity,
            records=len(data),
            compressed=compress,
            size_bytes=len(content)
        )
        
        # Upload metadata if provided
        if metadata:
            self._upload_metadata(partition_path, timestamp_str, metadata)
        
        return full_path
    
    def _upload_metadata(
        self,
        partition_path: str,
        timestamp_str: str,
        metadata: Dict[str, Any]
    ):
        """
        Upload metadata file.
        
        Args:
            partition_path: Partition path prefix
            timestamp_str: Timestamp string cho filename
            metadata: Metadata dictionary
        """
        metadata_path = f"{partition_path}_metadata/{timestamp_str}.json"
        metadata_blob = self.bucket.blob(metadata_path)
        
        metadata_content = json.dumps(metadata, ensure_ascii=False, default=str)
        metadata_blob.upload_from_string(
            metadata_content.encode('utf-8'),
            content_type='application/json'
        )
        
        logger.debug(f"Uploaded metadata", path=metadata_path)
    
    def upload_batch(
        self,
        entity: str,
        data_batches: List[List[Dict[str, Any]]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Upload nhiều batches của data.
        
        Args:
            entity: Tên entity
            data_batches: Danh sách các batches của data
            metadata: Metadata tùy chọn (chỉ upload 1 lần cho batch đầu tiên)
            
        Returns:
            List[str]: Danh sách các file paths đã upload
        """
        uploaded_paths = []
        
        for batch_idx, batch_data in enumerate(data_batches, 1):
            logger.info(
                f"Uploading batch {batch_idx}/{len(data_batches)}",
                batch=batch_idx,
                total_batches=len(data_batches),
                records=len(batch_data)
            )
            
            path = self.upload_json(
                entity=entity,
                data=batch_data,
                metadata=metadata if batch_idx == 1 else None  # Only upload metadata once
            )
            
            if path:
                uploaded_paths.append(path)
        
        logger.info(
            f"Completed batch upload: {len(uploaded_paths)} files",
            total_files=len(uploaded_paths),
            entity=entity
        )
        
        return uploaded_paths
