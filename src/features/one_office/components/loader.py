"""
Loader for 1Office Personnel Data.
Responsible for loading data into BigQuery.
"""
from datetime import date, datetime
from typing import List, Dict, Any
from google.cloud import bigquery
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class OneOfficeLoader:
    """
    Component chuyên trách việc load dữ liệu vào BigQuery.
    """
    
    def __init__(self):
        self.client = bigquery.Client(project=settings.gcp_project)
        self.dataset_id = settings.bronze_dataset # Use bronze dataset for raw/snapshot data
        self.table_name = "hr_profile_daily_snapshot"
        self.table_id = f"{settings.gcp_project}.{self.dataset_id}.{self.table_name}"
        
    def _create_table_if_not_exists(self):
        """
        Tạo bảng nếu chưa tồn tại.
        Schema được định nghĩa tại đây.
        """
        schema = [
            bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED", description="Ngày lấy dữ liệu"),
            bigquery.SchemaField("code", "STRING", mode="NULLABLE", description="Mã nhân sự"),
            bigquery.SchemaField("full_name", "STRING", mode="NULLABLE", description="Họ và tên"),
            bigquery.SchemaField("department_id", "STRING", mode="NULLABLE", description="ID Phòng ban"),
            bigquery.SchemaField("position_id", "STRING", mode="NULLABLE", description="ID Chức vụ"),
            bigquery.SchemaField("job_status", "STRING", mode="NULLABLE", description="Trạng thái công việc"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE", description="ID User hệ thống"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE", description="Email"),
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE", description="Số điện thoại"),
            # Lưu toàn bộ raw JSON để phòng hờ thay đổi schema hoặc cần fields khác sau này
            bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE", description="Dữ liệu gốc từ API"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="NULLABLE", description="Thời điểm insert bản ghi"),
        ]
        
        try:
            self.client.get_table(self.table_id)
            logger.info(f"Table {self.table_id} already exists.")
        except Exception:
            logger.info(f"Table {self.table_id} not found. Creating...")
            table = bigquery.Table(self.table_id, schema=schema)
            # Partitioning by snapshot_date is crucial for performance and cost
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="snapshot_date"
            )
            self.client.create_table(table)
            logger.info(f"Created table {self.table_id}")

    def load_snapshots(self, data: List[Dict[str, Any]]) -> int:
        """
        Load list hồ sơ vào BigQuery với snapshot_date là hôm nay.
        
        Args:
            data: List các dict dữ liệu từ API.
            
        Returns:
            int: Số lượng bản ghi đã insert.
        """
        if not data:
            logger.warning("No data to load.")
            return 0
            
        self._create_table_if_not_exists()
        
        today = date.today()
        rows_to_insert = []
        
        for item in data:
            # Transform nhẹ: Chọn fields quan trọng và đóng gói raw_data
            row = {
                "snapshot_date": today.isoformat(),
                "code": item.get("code"),
                "full_name": item.get("full_name") or item.get("fullname"), # API có thể trả về fullname
                "department_id": str(item.get("department_id", "")),
                "position_id": str(item.get("position_id", "")),
                "job_status": str(item.get("job_status", "")),
                "user_id": str(item.get("user_id", "")),
                "email": item.get("email"),
                "phone": item.get("phone") or item.get("mobile"),
                "raw_data": item, # BigQuery JSON type supports dict directly
                "inserted_at": datetime.now().isoformat()
            }
            rows_to_insert.append(row)
            
        # Sử dụng insert_rows_json (streaming). 
        # Với lượng dữ liệu nhân sự (thường < 10k), streaming là ổn và nhanh gọn.
        # Nếu data lớn (>100k), nên dùng LoadJob với file NDJSON.
        
        errors = self.client.insert_rows_json(self.table_id, rows_to_insert)
        
        if errors:
            logger.error(f"Encountered {len(errors)} errors while inserting rows: {errors[:5]}...")
            raise Exception("BigQuery insert failed")
            
        logger.info(f"Successfully inserted {len(rows_to_insert)} rows into {self.table_id}")
        return len(rows_to_insert)
