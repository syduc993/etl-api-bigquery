"""
GCS Loader với hỗ trợ partitioning.
File này xử lý việc upload data lên Google Cloud Storage với:
- Partitioning theo thời gian (month hoặc day level)
- Parquet format cho hiệu suất tốt hơn
- Metadata tracking
- Idempotent uploads (không duplicate nếu file đã tồn tại)
- Explicit schema enforcement để tránh schema evolution issues
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
from src.shared.logging import get_logger
from src.shared.parquet.schemas import get_schema

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
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Ensure the GCS bucket exists."""
        try:
            if not self.bucket.exists():
                logger.info(f"Bucket {self.bucket_name} not found, creating...")
                self.bucket.create(location=settings.gcp_region)
                logger.info(f"Created bucket {self.bucket_name}")
        except Exception as e:
            # If we don't have permission to list/create, we just log and try to proceed
            logger.warning(f"Could not verify/create bucket {self.bucket_name}: {e}")
    
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
            partition_path: Partition path prefix
            file_extension: Extension của file cần xóa
            date_filter: Nếu có, chỉ xóa file có chứa date này trong filename
            
        Returns:
            int: Số lượng file đã xóa
        """
        deleted_count = 0
        blobs = self.bucket.list_blobs(prefix=partition_path)
        
        date_pattern = None
        if date_filter:
            date_pattern = f"data_{date_filter.isoformat()}_"
        
        for blob in blobs:
            if blob.name.endswith(file_extension) and not blob.name.endswith('/'):
                if date_pattern and date_pattern not in blob.name:
                    continue
                
                try:
                    blob.delete()
                    deleted_count += 1
                    logger.debug(
                        f"Deleted existing file in partition",
                        path=blob.name,
                        partition=partition_path
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete file in partition",
                        path=blob.name,
                        error=str(e)
                    )
        
        if deleted_count > 0:
            logger.info(
                f"Deleted {deleted_count} existing file(s) in partition",
                deleted_count=deleted_count,
                partition=partition_path
            )
        
        return deleted_count
    
    def _upload_metadata(
        self,
        partition_path: str,
        timestamp_str: str,
        metadata: Dict[str, Any]
    ):
        """Upload metadata file."""
        metadata_path = f"{partition_path}_metadata/{timestamp_str}.json"
        metadata_blob = self.bucket.blob(metadata_path)
        
        metadata_content = json.dumps(metadata, ensure_ascii=False, default=str)
        metadata_blob.upload_from_string(
            metadata_content.encode('utf-8'),
            content_type='application/json'
        )
        
        logger.debug(f"Uploaded metadata", path=metadata_path)
    
    def upload_parquet(
        self,
        entity: str,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        metadata: Optional[Dict[str, Any]] = None,
        overwrite_partition: bool = True,
        schema: Optional[pa.Schema] = None
    ) -> str:
        """
        Upload data dưới dạng Parquet lên GCS với partitioning.
        
        Args:
            entity: Tên entity (format: "platform/entity", e.g., "nhanh/bill_products")
            data: Danh sách records để upload
            partition_date: Ngày để partition (mặc định: hôm nay)
            metadata: Metadata tùy chọn
            overwrite_partition: Nếu True, xóa file cũ trước khi upload
            schema: Explicit PyArrow schema (nếu None, sẽ lookup từ registry hoặc infer)
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning(f"No data to upload for {entity}", entity=entity)
            return ""
        
        if partition_date is None:
            partition_date = datetime.utcnow().date()
        
        partition_datetime = datetime.combine(partition_date, datetime.min.time())
        partition_path = self._get_partition_path(entity, partition_datetime)
        
        if overwrite_partition:
            self._delete_partition_files(
                partition_path, 
                file_extension=".parquet",
                date_filter=partition_date
            )
        
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"data_{partition_date.isoformat()}_{timestamp_str}.parquet"
        full_path = f"{partition_path}{filename}"
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Normalize timestamps trong DataFrame: convert datetime64[ns] → datetime64[us]
        # BigQuery TIMESTAMP chỉ hỗ trợ microsecond precision, không hỗ trợ nanosecond
        # PyArrow sẽ tự động tạo nanosecond precision nếu DataFrame có datetime64[ns]
        # Fix này đảm bảo tất cả timestamp columns có microsecond precision
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                # Convert nanosecond precision to microsecond precision
                # Use astype to force datetime64[us] instead of just floor
                df[col] = df[col].dt.floor('us').astype('datetime64[us]')
        
        # Get schema: explicit > registry lookup > inference
        if schema is None:
            schema = get_schema(entity)
        
        # Create PyArrow table with or without explicit schema
        if schema:
            # CRITICAL FIX: Đảm bảo DataFrame có TẤT CẢ schema fields trước khi write Parquet
            # Nếu thiếu cột nào, thêm vào với giá trị None để đảm bảo schema consistency
            schema_column_names = {field.name for field in schema}
            df_column_names = set(df.columns)
            
            # Thêm các cột thiếu với giá trị None
            missing_columns = schema_column_names - df_column_names
            if missing_columns:
                for col in missing_columns:
                    df[col] = None
                logger.debug(
                    f"Added missing schema columns to DataFrame",
                    entity=entity,
                    missing_columns=list(missing_columns),
                    total_schema_fields=len(schema_column_names),
                    df_columns_before=len(df_column_names),
                    df_columns_after=len(df.columns)
                )
            
            # Use explicit schema - enforces types and handles coercion
            # Giờ tất cả schema fields đã có trong DataFrame, không cần filter nữa
            # Nhưng vẫn filter để đảm bảo chỉ dùng fields có trong schema (tránh extra fields)
            df_columns = set(df.columns)
            schema_fields = [field for field in schema if field.name in df_columns]
            
            if schema_fields:
                # Create filtered schema with only fields present in data
                # Override timestamp fields to use microsecond precision (not nanosecond)
                schema_fields_fixed = []
                for field in schema_fields:
                    if pa.types.is_timestamp(field.type):
                        # Force microsecond precision for BigQuery compatibility
                        schema_fields_fixed.append(
                            pa.field(field.name, pa.timestamp('us'), nullable=field.nullable)
                        )
                    else:
                        schema_fields_fixed.append(field)
                
                filtered_schema = pa.schema(schema_fields_fixed)
                table = pa.Table.from_pandas(df, schema=filtered_schema)
                logger.debug(
                    f"Using explicit schema for Parquet write",
                    entity=entity,
                    schema_fields=len(schema_fields),
                    total_schema_fields=len(schema)
                )
            else:
                # No matching fields, fallback to inference
                table = pa.Table.from_pandas(df)
                logger.debug(
                    f"Schema defined but no matching fields in data, using inference",
                    entity=entity
                )
        else:
            # Fallback to inference (backward compatibility)
            table = pa.Table.from_pandas(df)
            logger.debug(
                f"Using inferred schema for Parquet write",
                entity=entity
            )
        
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_bytes = parquet_buffer.getvalue()
        
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
            size_bytes=len(parquet_bytes),
            schema_enforced=(schema is not None)
        )
        
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
        overwrite_partition: bool = True,
        schema: Optional[pa.Schema] = None
    ) -> str:
        """
        Upload data dưới dạng Parquet, group theo ngày từ date_field.
        
        Args:
            entity: Tên entity (format: "platform/entity")
            data: Danh sách records
            partition_date: Ngày để partition (nếu None, parse từ date_field)
            date_field: Tên field chứa date
            metadata: Metadata tùy chọn
            overwrite_partition: Nếu True, xóa file cũ
            schema: Explicit PyArrow schema (nếu None, sẽ lookup từ registry)
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning(f"No data to upload for {entity}", entity=entity)
            return ""
        
        if partition_date is None:
            if date_field in data[0] and data[0][date_field]:
                try:
                    date_str = data[0][date_field]
                    if isinstance(date_str, str):
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
                        error=str(e)
                    )
                    partition_date = datetime.utcnow().date()
            else:
                partition_date = datetime.utcnow().date()
        
        return self.upload_parquet(
            entity=entity,
            data=data,
            partition_date=partition_date,
            metadata=metadata,
            overwrite_partition=overwrite_partition,
            schema=schema
        )
