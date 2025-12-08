"""
Watermark tracking cho incremental extraction.
File này quản lý việc track timestamp của lần extraction cuối cùng
để hỗ trợ incremental extraction (chỉ lấy data mới).
Watermark được lưu trong BigQuery table.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional
from google.cloud import bigquery
from src.config import settings
from src.utils.logging import get_logger
from src.utils.exceptions import WatermarkError

logger = get_logger(__name__)


class WatermarkTracker:
    """
    Track extraction watermarks cho incremental processing.
    
    Watermark được lưu trong BigQuery table với thông tin:
    - entity: Tên entity (bills, products, customers)
    - last_extracted_at: Timestamp của lần extraction cuối
    - last_successful_run: Timestamp của lần chạy thành công cuối
    - records_count: Số lượng records đã extract
    """
    
    def __init__(self):
        """
        Khởi tạo watermark tracker.
        
        Tự động tạo BigQuery table nếu chưa tồn tại.
        """
        self.client = bigquery.Client(project=settings.gcp_project)
        self.dataset_id = settings.bronze_dataset
        self.table_id = "extraction_watermarks"
        self._ensure_table()
    
    def _ensure_table(self):
        """
        Đảm bảo watermark table tồn tại.
        
        Tạo table với schema nếu chưa có.
        """
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)
        
        try:
            self.client.get_table(table_ref)
        except Exception:
            # Create table if it doesn't exist
            schema = [
                bigquery.SchemaField("entity", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_extracted_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("last_successful_run", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("records_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            
            logger.info(f"Created watermark table: {self.table_id}")
    
    def get_watermark(self, entity: str) -> Optional[datetime]:
        """
        Lấy watermark của lần extraction cuối cùng cho entity.
        
        Args:
            entity: Tên entity (ví dụ: 'bills', 'products', 'customers')
            
        Returns:
            Optional[datetime]: Timestamp của lần extraction cuối, hoặc None nếu chưa có
            
        Raises:
            WatermarkError: Nếu có lỗi khi query BigQuery
        """
        query = f"""
        SELECT last_extracted_at
        FROM `{settings.gcp_project}.{self.dataset_id}.{self.table_id}`
        WHERE entity = @entity
        ORDER BY last_extracted_at DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("entity", "STRING", entity)
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            row = next(results, None)
            if row:
                # Ensure timezone-aware datetime
                watermark = row.last_extracted_at
                if watermark and watermark.tzinfo is None:
                    watermark = watermark.replace(tzinfo=timezone.utc)
                return watermark
            return None
        
        except Exception as e:
            logger.error(f"Error getting watermark for {entity}", error=str(e))
            raise WatermarkError(f"Failed to get watermark: {str(e)}")
    
    def update_watermark(
        self,
        entity: str,
        extracted_at: datetime,
        records_count: Optional[int] = None
    ):
        """
        Cập nhật watermark sau khi extraction thành công.
        
        Args:
            entity: Tên entity
            extracted_at: Timestamp của extraction
            records_count: Số lượng records đã extract (tùy chọn)
            
        Raises:
            WatermarkError: Nếu có lỗi khi update BigQuery
        """
        now = datetime.now(timezone.utc)
        
        query = f"""
        MERGE `{settings.gcp_project}.{self.dataset_id}.{self.table_id}` AS target
        USING (
            SELECT
                @entity AS entity,
                @extracted_at AS last_extracted_at,
                @now AS last_successful_run,
                @records_count AS records_count,
                @now AS updated_at
        ) AS source
        ON target.entity = source.entity
        WHEN MATCHED THEN
            UPDATE SET
                last_extracted_at = source.last_extracted_at,
                last_successful_run = source.last_successful_run,
                records_count = source.records_count,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (entity, last_extracted_at, last_successful_run, records_count, updated_at)
            VALUES (source.entity, source.last_extracted_at, source.last_successful_run, source.records_count, source.updated_at)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
                bigquery.ScalarQueryParameter("extracted_at", "TIMESTAMP", extracted_at),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("records_count", "INTEGER", records_count),
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            
            logger.info(
                f"Updated watermark for {entity}",
                entity=entity,
                extracted_at=extracted_at.isoformat(),
                records_count=records_count
            )
        
        except Exception as e:
            logger.error(f"Error updating watermark for {entity}", error=str(e))
            raise WatermarkError(f"Failed to update watermark: {str(e)}")
    
    def get_incremental_range(
        self,
        entity: str,
        lookback_hours: int = 1
    ) -> tuple[Optional[datetime], datetime]:
        """
        Lấy date range cho incremental extraction.
        
        Args:
            entity: Tên entity
            lookback_hours: Số giờ lookback nếu không có watermark (mặc định: 1 giờ)
            
        Returns:
            tuple: (from_date, to_date) - from_date có thể là None nếu không có watermark
        """
        watermark = self.get_watermark(entity)
        to_date = datetime.now(timezone.utc)
        
        if watermark:
            # Ensure both are timezone-aware
            if watermark.tzinfo is None:
                watermark = watermark.replace(tzinfo=timezone.utc)
            from_date = watermark
            logger.info(
                f"Using watermark for incremental extraction",
                entity=entity,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
        else:
            # No watermark: use lookback period
            from_date = to_date - timedelta(hours=lookback_hours)
            logger.info(
                f"No watermark found, using lookback period",
                entity=entity,
                lookback_hours=lookback_hours,
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
        
        return from_date, to_date
