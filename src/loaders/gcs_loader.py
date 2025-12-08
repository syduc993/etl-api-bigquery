"""
GCS Loader với hỗ trợ partitioning.
File này xử lý việc upload data lên Google Cloud Storage với:
- Partitioning theo thời gian (month hoặc day level)
- Parquet format cho hiệu suất tốt hơn
- Metadata tracking
- Idempotent uploads (không duplicate nếu file đã tồn tại)
"""
import json
import gzip
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


class GCSLoader:
    """
    Loader để upload data lên GCS với partitioning.
    
    Hỗ trợ 2 loại partitioning:
    - Month level: year={YYYY}/month={MM} (đơn giản hơn)
    - Day level: year={YYYY}/month={MM}/day={DD} (performance tốt hơn)
    
    Behavior khi re-run:
    - Mặc định (overwrite_partition=True): Xóa tất cả file parquet cũ trong partition,
      sau đó upload file mới. Đảm bảo mỗi partition chỉ có 1 file mới nhất.
    - Nếu overwrite_partition=False: Giữ lại file cũ, tạo file mới với timestamp khác.
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
        # For BigQuery compatibility, use newline-delimited JSON (NDJSON)
        # Each line is a separate JSON object
        json_lines = [json.dumps(record, ensure_ascii=False, default=str) for record in data]
        json_content = '\n'.join(json_lines)
        
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
    
    def _delete_partition_files(
        self,
        partition_path: str,
        file_extension: str = ".parquet",
        date_filter: Optional[date] = None
    ) -> int:
        """
        Xóa file có extension cụ thể trong partition path.
        
        Args:
            partition_path: Partition path prefix (ví dụ: 'bills/year=2025/month=12/')
            file_extension: Extension của file cần xóa (mặc định: '.parquet')
            date_filter: Nếu có, chỉ xóa file có chứa date này trong filename
                         (ví dụ: date_filter=2025-12-01 sẽ chỉ xóa file có 'data_2025-12-01_' trong tên)
                         Nếu None, xóa tất cả file trong partition
            
        Returns:
            int: Số lượng file đã xóa
        """
        deleted_count = 0
        
        # List all blobs in partition path
        blobs = self.bucket.list_blobs(prefix=partition_path)
        
        # Build date filter pattern if provided
        date_pattern = None
        if date_filter:
            date_pattern = f"data_{date_filter.isoformat()}_"
        
        for blob in blobs:
            # Only delete files with matching extension (skip folders and metadata)
            if blob.name.endswith(file_extension) and not blob.name.endswith('/'):
                # If date_filter is provided, only delete files matching that date
                if date_pattern and date_pattern not in blob.name:
                    continue
                
                try:
                    blob.delete()
                    deleted_count += 1
                    logger.debug(
                        f"Deleted existing file in partition",
                        path=blob.name,
                        partition=partition_path,
                        date_filter=date_filter.isoformat() if date_filter else None
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete file in partition",
                        path=blob.name,
                        error=str(e),
                        partition=partition_path
                    )
        
        if deleted_count > 0:
            logger.info(
                f"Deleted {deleted_count} existing file(s) in partition",
                deleted_count=deleted_count,
                partition=partition_path,
                extension=file_extension,
                date_filter=date_filter.isoformat() if date_filter else "all files"
            )
        
        return deleted_count
    
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
    
    def upload_parquet(
        self,
        entity: str,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        metadata: Optional[Dict[str, Any]] = None,
        overwrite_partition: bool = True
    ) -> str:
        """
        Upload data dưới dạng Parquet lên GCS với partitioning theo config.
        
        Args:
            entity: Tên entity (ví dụ: 'bills', 'products', 'customers')
            data: Danh sách records để upload
            partition_date: Ngày để partition (mặc định: hôm nay)
            metadata: Metadata tùy chọn
            overwrite_partition: Nếu True, xóa tất cả file parquet cũ trong partition trước khi upload
            
        Returns:
            str: GCS path của file đã upload (rỗng nếu không có data)
        """
        if not data:
            logger.warning(f"No data to upload for {entity}", entity=entity)
            return ""
        
        # Use partition_date or current date
        if partition_date is None:
            partition_date = datetime.utcnow().date()
        
        # Convert date to datetime for _get_partition_path (uses datetime at midnight)
        partition_datetime = datetime.combine(partition_date, datetime.min.time())
        
        # Create partition path using configured strategy (month or day)
        partition_path = self._get_partition_path(entity, partition_datetime)
        
        # If overwrite_partition, delete existing parquet files
        # - If month-level partitioning: only delete files for this specific date (from filename)
        # - If day-level partitioning: delete all files in the day partition
        if overwrite_partition:
            # Always pass date_filter to only delete files matching this date
            # This ensures month-level partitioning doesn't delete other days' files
            self._delete_partition_files(
                partition_path, 
                file_extension=".parquet",
                date_filter=partition_date
            )
        
        # Generate filename with date
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"data_{partition_date.isoformat()}_{timestamp_str}.parquet"
        full_path = f"{partition_path}{filename}"
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert DataFrame to Parquet
        parquet_buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_bytes = parquet_buffer.getvalue()
        
        # Upload to GCS
        blob = self.bucket.blob(full_path)
        blob.upload_from_string(
            parquet_bytes,
            content_type='application/parquet'
        )
        
        logger.info(
            f"Uploaded {len(data)} records to GCS as Parquet",
            path=full_path,
            entity=entity,
            records=len(data),
            partition_date=partition_date.isoformat(),
            size_bytes=len(parquet_bytes)
        )
        
        # Upload metadata if provided
        if metadata:
            self._upload_metadata(partition_path, timestamp_str, metadata)
        
        return full_path
    
    def upload_parquet_by_date(
        self,
        entity: str,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        date_field: str = "date",
        metadata: Optional[Dict[str, Any]] = None,
        overwrite_partition: bool = True
    ) -> str:
        """
        Upload data dưới dạng Parquet, group theo ngày từ date_field.
        Nếu partition_date được cung cấp, dùng nó thay vì parse từ data.
        
        Args:
            entity: Tên entity
            data: Danh sách records
            partition_date: Ngày để partition (nếu None, sẽ parse từ date_field)
            date_field: Tên field chứa date để group (mặc định: "date")
            metadata: Metadata tùy chọn
            overwrite_partition: Nếu True, xóa tất cả file parquet cũ trong partition trước khi upload
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning(f"No data to upload for {entity}", entity=entity)
            return ""
        
        # Determine partition date
        if partition_date is None:
            # Try to extract date from first record
            if date_field in data[0] and data[0][date_field]:
                try:
                    date_str = data[0][date_field]
                    if isinstance(date_str, str):
                        # Try ISO format first
                        if 'T' in date_str:
                            partition_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
                        else:
                            partition_date = datetime.strptime(date_str.split()[0], '%Y-%m-%d').date()
                    elif isinstance(date_str, datetime):
                        partition_date = date_str.date()
                except Exception as e:
                    logger.warning(
                        f"Could not parse date from {date_field}, using today",
                        field=date_field,
                        value=data[0].get(date_field),
                        error=str(e)
                    )
                    partition_date = datetime.utcnow().date()
            else:
                partition_date = datetime.utcnow().date()
        
        # Upload as single Parquet file for this date
        path = self.upload_parquet(
            entity=entity,
            data=data,
            partition_date=partition_date,
            metadata=metadata,
            overwrite_partition=overwrite_partition
        )
        
        return path
