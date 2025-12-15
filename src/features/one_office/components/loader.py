"""
Loader for 1Office Personnel Data.
Responsible for loading data into BigQuery.
"""
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import json
import gzip
from google.cloud import bigquery, storage
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def parse_date_ddmmyyyy(date_str: Optional[str]) -> Optional[str]:
    """
    Convert date từ dd/mm/YYYY sang YYYY-MM-DD cho BigQuery.
    Chỉ parse format chuẩn dd/mm/YYYY, bỏ qua các giá trị không hợp lệ.
    
    Args:
        date_str: Date string dạng "dd/mm/YYYY" hoặc None/empty
        
    Returns:
        str: Date string dạng "YYYY-MM-DD" hoặc None nếu không parse được
    """
    if not date_str or date_str == "":
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Skip nếu có ký tự đặc biệt (như "dự kiến", parentheses, etc.)
        if any(char in date_str for char in ['(', ')', 'dự kiến', 'expected']):
            return None
        
        # Parse dd/mm/YYYY
        parts = date_str.split('/')
        if len(parts) == 3:
            day, month, year = parts
            day = day.strip()
            month = month.strip()
            year = year.strip()
            
            # Validate là số
            if not (day.isdigit() and month.isdigit() and year.isdigit()):
                return None
            
            # Validate range
            day_int = int(day)
            month_int = int(month)
            year_int = int(year)
            
            if not (1 <= day_int <= 31 and 1 <= month_int <= 12 and 1900 <= year_int <= 2100):
                return None
            
            # Format YYYY-MM-DD
            return f"{year_int}-{month.zfill(2)}-{day.zfill(2)}"
    except Exception:
        pass
    return None


def safe_int(value: Any) -> Optional[int]:
    """Convert value to int safely."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float(value: Any) -> Optional[float]:
    """Convert value to float safely."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_str(value: Any) -> Optional[str]:
    """Convert value to string safely, return None for empty."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


class OneOfficeLoader:
    """
    Component chuyên trách việc load dữ liệu vào BigQuery.
    Lưu raw JSON data vào GCS trước khi load vào BigQuery để có backup.
    """
    
    def __init__(self):
        self.client = bigquery.Client(project=settings.gcp_project)
        self.dataset_id = settings.oneoffice_dataset  # Use oneoffice dataset for native tables
        self.table_name = "hr_profile_daily_snapshot"
        self.table_id = f"{settings.gcp_project}.{self.dataset_id}.{self.table_name}"
        # GCS loader để upload raw data
        self.storage_client = storage.Client(project=settings.gcp_project)
        self.bucket = self.storage_client.bucket(settings.bronze_bucket)
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
    
    def _ensure_dataset_exists(self):
        """
        Tạo dataset nếu chưa tồn tại.
        """
        try:
            dataset_id = f"{settings.gcp_project}.{self.dataset_id}"
            dataset = self.client.get_dataset(dataset_id)
            logger.debug(f"Dataset {dataset_id} already exists.")
        except Exception:
            logger.info(f"Dataset {self.dataset_id} not found. Creating...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = settings.gcp_region
            dataset.description = "1Office HR data - Native tables for daily snapshots"
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")
        
    def _create_table_if_not_exists(self):
        """
        Tạo bảng nếu chưa tồn tại.
        Schema đầy đủ với tất cả fields từ API.
        """
        schema = [
            # Metadata
            bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED", description="Ngày lấy dữ liệu"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="NULLABLE", description="Thời điểm insert bản ghi"),
            
            # Basic Info
            bigquery.SchemaField("code", "STRING", mode="NULLABLE", description="Mã nhân sự"),
            bigquery.SchemaField("full_name", "STRING", mode="NULLABLE", description="Họ và tên"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE", description="Email"),
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE", description="Số điện thoại"),
            
            # Job Information
            bigquery.SchemaField("department_id", "STRING", mode="NULLABLE", description="ID Phòng ban"),
            bigquery.SchemaField("department_business", "STRING", mode="NULLABLE", description="Khối nghiệp vụ"),
            bigquery.SchemaField("department_type_id", "STRING", mode="NULLABLE", description="Loại phòng ban"),
            bigquery.SchemaField("position_id", "STRING", mode="NULLABLE", description="ID Vị trí"),
            bigquery.SchemaField("job_title", "STRING", mode="NULLABLE", description="Chức vụ"),
            bigquery.SchemaField("job_status", "STRING", mode="NULLABLE", description="Trạng thái công việc"),
            bigquery.SchemaField("job_date_join", "DATE", mode="NULLABLE", description="Ngày vào"),
            bigquery.SchemaField("job_reldate_join", "DATE", mode="NULLABLE", description="Ngày ký HĐLĐ chính thức"),
            bigquery.SchemaField("job_date_end_review", "DATE", mode="NULLABLE", description="Ngày hết hạn thử việc"),
            bigquery.SchemaField("job_date_out", "DATE", mode="NULLABLE", description="Ngày nghỉ việc"),
            bigquery.SchemaField("job_out_reason", "STRING", mode="NULLABLE", description="Lý do nghỉ"),
            bigquery.SchemaField("job_contract", "STRING", mode="NULLABLE", description="Tên hợp đồng"),
            bigquery.SchemaField("type_contract", "STRING", mode="NULLABLE", description="Loại hợp đồng"),
            bigquery.SchemaField("year_join", "INT64", mode="NULLABLE", description="Thâm niên"),
            bigquery.SchemaField("year_work_experience_now", "INT64", mode="NULLABLE", description="Số năm kinh nghiệm hiện tại"),
            bigquery.SchemaField("date_join_now", "DATE", mode="NULLABLE", description="Ngày vào công ty hàng năm"),
            bigquery.SchemaField("concurrent_position_ids", "STRING", mode="NULLABLE", description="Vị trí kiêm nhiệm"),
            
            # Personal Information
            bigquery.SchemaField("birthday", "DATE", mode="NULLABLE", description="Ngày sinh"),
            bigquery.SchemaField("birthday_now", "DATE", mode="NULLABLE", description="Ngày sinh nhật hiện tại"),
            bigquery.SchemaField("birthday_current", "INT64", mode="NULLABLE", description="Số tuổi"),
            bigquery.SchemaField("gender", "STRING", mode="NULLABLE", description="Giới tính"),
            bigquery.SchemaField("marital_status", "STRING", mode="NULLABLE", description="Tình trạng hôn nhân"),
            bigquery.SchemaField("military_service", "STRING", mode="NULLABLE", description="Nghĩa vụ quân sự"),
            bigquery.SchemaField("people", "STRING", mode="NULLABLE", description="Dân tộc"),
            bigquery.SchemaField("religious", "STRING", mode="NULLABLE", description="Tôn giáo"),
            
            # Documents
            bigquery.SchemaField("private_code_date", "DATE", mode="NULLABLE", description="Ngày cấp CCCD"),
            bigquery.SchemaField("image_private_front", "STRING", mode="NULLABLE", description="Ảnh CCCD/CMND mặt trước"),
            bigquery.SchemaField("image_private_back", "STRING", mode="NULLABLE", description="Ảnh CCCD/CMND mặt sau"),
            bigquery.SchemaField("passport_date_from", "DATE", mode="NULLABLE", description="Ngày cấp hộ chiếu"),
            bigquery.SchemaField("passport_date_expire", "DATE", mode="NULLABLE", description="Ngày hết hạn hộ chiếu"),
            bigquery.SchemaField("passport_code_place", "STRING", mode="NULLABLE", description="Nơi cấp hộ chiếu"),
            bigquery.SchemaField("passport_type", "STRING", mode="NULLABLE", description="Loại hộ chiếu"),
            bigquery.SchemaField("num_file", "INT64", mode="NULLABLE", description="Số lượng file đính kèm"),
            bigquery.SchemaField("photo", "STRING", mode="NULLABLE", description="Ảnh đại diện"),
            
            # System & Audit
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE", description="Tài khoản 1Office"),
            bigquery.SchemaField("raw_user_id", "INT64", mode="NULLABLE", description="ID người dùng"),
            bigquery.SchemaField("date_trigger_user", "DATE", mode="NULLABLE", description="Ngày tạo TK 1Office"),
            bigquery.SchemaField("date_created", "DATE", mode="NULLABLE", description="Ngày tạo"),
            bigquery.SchemaField("date_updated", "DATE", mode="NULLABLE", description="Ngày sửa hồ sơ"),
            bigquery.SchemaField("created_by_id", "STRING", mode="NULLABLE", description="Người tạo"),
            bigquery.SchemaField("updated_by_id", "STRING", mode="NULLABLE", description="Người sửa"),
            
            # Salary
            bigquery.SchemaField("salary_real", "FLOAT64", mode="NULLABLE", description="Lương vị trí"),
            bigquery.SchemaField("date_change_salary", "DATE", mode="NULLABLE", description="Ngày tăng lương"),
            bigquery.SchemaField("level_id", "STRING", mode="NULLABLE", description="Cấp bậc"),
            bigquery.SchemaField("coefficient", "FLOAT64", mode="NULLABLE", description="Hệ số tay nghề"),
            bigquery.SchemaField("salary_area_id", "STRING", mode="NULLABLE", description="Vùng áp dụng lương tối thiểu"),
            bigquery.SchemaField("salary_min_area", "FLOAT64", mode="NULLABLE", description="Lương tối thiểu vùng"),
            
            # Other
            bigquery.SchemaField("desc", "STRING", mode="NULLABLE", description="Ghi chú (HTML)"),
            bigquery.SchemaField("level_academic", "STRING", mode="NULLABLE", description="Trình độ học vấn cao nhất"),
            bigquery.SchemaField("level_school", "STRING", mode="NULLABLE", description="Trình độ phổ thông"),
            bigquery.SchemaField("job_bank_id", "STRING", mode="NULLABLE", description="Ngân hàng"),
            bigquery.SchemaField("gps_location_ids", "STRING", mode="NULLABLE", description="Địa điểm chấm công GPS"),
            bigquery.SchemaField("labor_group", "STRING", mode="NULLABLE", description="Loại lao động"),
            bigquery.SchemaField("is_econtract", "STRING", mode="NULLABLE", description="Ký số"),
            bigquery.SchemaField("live_manager_id", "STRING", mode="NULLABLE", description="Người quản lý trực tiếp"),
            bigquery.SchemaField("certificate_status", "STRING", mode="NULLABLE", description="Trạng thái chữ ký số"),
            bigquery.SchemaField("date_created_certificate", "DATE", mode="NULLABLE", description="Ngày cấp chữ ký số"),
            bigquery.SchemaField("certificate", "STRING", mode="NULLABLE", description="Chữ ký số"),
            bigquery.SchemaField("work_place", "STRING", mode="NULLABLE", description="Nơi làm việc"),
            bigquery.SchemaField("date_leave_from", "DATE", mode="NULLABLE", description="Nghỉ việc từ ngày"),
            bigquery.SchemaField("date_leave_to", "DATE", mode="NULLABLE", description="Nghỉ việc đến ngày"),
            bigquery.SchemaField("due_date", "DATE", mode="NULLABLE", description="Ngày dự sinh"),
            
            # Raw data backup
            bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE", description="Dữ liệu gốc từ API"),
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

    def _delete_partition_files(self, partition_path: str, file_prefix: str = "timestamp_"):
        """
        Xóa các file cũ trong partition trước khi upload file mới.
        Đảm bảo mỗi partition (ngày) chỉ có 1 file mới nhất.
        
        Args:
            partition_path: Partition path (vd: oneoffice/hr_profile/year=2025/month=12/day=13/)
            file_prefix: Prefix của files cần xóa (default: timestamp_)
        """
        try:
            blobs = list(self.bucket.list_blobs(prefix=partition_path))
            deleted_count = 0
            for blob in blobs:
                if blob.name.startswith(f"{partition_path}{file_prefix}"):
                    blob.delete()
                    deleted_count += 1
                    logger.debug(f"Deleted old file: {blob.name}")
            
            if deleted_count > 0:
                logger.info(
                    f"Cleaned up {deleted_count} old file(s) in partition",
                    partition_path=partition_path,
                    deleted_count=deleted_count
                )
        except Exception as e:
            logger.warning(
                f"Failed to cleanup old files in partition, continuing",
                partition_path=partition_path,
                error=str(e)
            )
    
    def _upload_raw_json_to_gcs(
        self, 
        data: List[Dict[str, Any]], 
        snapshot_date: date
    ) -> str:
        """
        Upload raw JSON data lên GCS trước khi transform.
        Xóa các file cũ trong cùng partition để đảm bảo mỗi ngày chỉ có 1 file mới nhất.
        
        Args:
            data: Raw data từ API
            snapshot_date: Ngày snapshot để partition
            
        Returns:
            str: GCS path của file đã upload (rỗng nếu không có data)
        """
        if not data:
            return ""
        
        try:
            # Convert snapshot_date to datetime at start of day for partition path
            partition_datetime = datetime.combine(snapshot_date, datetime.min.time())
            
            # Create partition path theo day strategy: oneoffice/hr_profile/year=YYYY/month=MM/day=DD/
            entity = "oneoffice/hr_profile"
            partition_path = (
                f"{entity}/"
                f"year={partition_datetime.year}/"
                f"month={partition_datetime.month:02d}/"
                f"day={partition_datetime.day:02d}/"
            )
            
            # Cleanup old files trong partition trước khi upload file mới
            self._delete_partition_files(partition_path)
            
            # Generate filename với timestamp để unique
            timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"timestamp_{timestamp_str}.json.gz"
            full_path = f"{partition_path}{filename}"
            
            # Prepare JSON content (NDJSON format)
            json_lines = [json.dumps(record, ensure_ascii=False, default=str) for record in data]
            json_content = '\n'.join(json_lines)
            
            # Compress với gzip
            content = gzip.compress(json_content.encode('utf-8'))
            
            # Upload to GCS
            blob = self.bucket.blob(full_path)
            blob.upload_from_string(content, content_type='application/gzip')
            
            logger.info(
                f"Uploaded raw JSON data to GCS",
                path=full_path,
                records=len(data),
                size_bytes=len(content),
                snapshot_date=snapshot_date.isoformat()
            )
            
            return full_path
            
        except Exception as e:
            # Log warning nhưng không block pipeline nếu GCS upload fail
            logger.warning(
                f"Failed to upload raw JSON to GCS, continuing with BigQuery load",
                error=str(e),
                snapshot_date=snapshot_date.isoformat()
            )
            return ""

    def load_snapshots(self, data: List[Dict[str, Any]]) -> int:
        """
        Load list hồ sơ vào BigQuery với snapshot_date là hôm nay.
        
        Flow:
        1. Upload raw JSON data lên GCS (backup)
        2. Transform và streaming insert vào BigQuery
        
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
        
        # Step 1: Upload raw JSON data lên GCS trước (backup raw data)
        gcs_path = self._upload_raw_json_to_gcs(data, today)
        if gcs_path:
            logger.info(f"Raw data backed up to GCS: gs://{settings.bronze_bucket}/{gcs_path}")
        rows_to_insert = []
        
        for item in data:
            # Extract tất cả fields từ API response
            row = {
                # Metadata
                "snapshot_date": today.isoformat(),
                "inserted_at": datetime.now().isoformat(),
                
                # Basic Info
                "code": safe_str(item.get("code")),
                "full_name": safe_str(item.get("full_name") or item.get("fullname")),  # API có thể trả về fullname
                "email": safe_str(item.get("email")),
                "phone": safe_str(item.get("phone") or item.get("mobile")),
                
                # Job Information
                "department_id": safe_str(item.get("department_id")),
                "department_business": safe_str(item.get("department_business")),
                "department_type_id": safe_str(item.get("department_type_id")),
                "position_id": safe_str(item.get("position_id")),
                "job_title": safe_str(item.get("job_title")),
                "job_status": safe_str(item.get("job_status")),
                "job_date_join": parse_date_ddmmyyyy(item.get("job_date_join")),
                "job_reldate_join": parse_date_ddmmyyyy(item.get("job_reldate_join")),
                "job_date_end_review": parse_date_ddmmyyyy(item.get("job_date_end_review")),
                "job_date_out": parse_date_ddmmyyyy(item.get("job_date_out")),
                "job_out_reason": safe_str(item.get("job_out_reason")),
                "job_contract": safe_str(item.get("job_contract")),
                "type_contract": safe_str(item.get("type_contract")),
                "year_join": safe_int(item.get("year_join")),
                "year_work_experience_now": safe_int(item.get("year_work_experience_now")),
                "date_join_now": parse_date_ddmmyyyy(item.get("date_join_now")),
                "concurrent_position_ids": safe_str(item.get("concurrent_position_ids")),
                
                # Personal Information
                "birthday": parse_date_ddmmyyyy(item.get("birthday")),
                "birthday_now": parse_date_ddmmyyyy(item.get("birthday_now")),
                "birthday_current": safe_int(item.get("birthday_current")),
                "gender": safe_str(item.get("gender")),
                "marital_status": safe_str(item.get("marital_status")),
                "military_service": safe_str(item.get("military_service")),
                "people": safe_str(item.get("people")),
                "religious": safe_str(item.get("religious")),
                
                # Documents
                "private_code_date": parse_date_ddmmyyyy(item.get("private_code_date")),
                "image_private_front": safe_str(item.get("image_private_front")),
                "image_private_back": safe_str(item.get("image_private_back")),
                "passport_date_from": parse_date_ddmmyyyy(item.get("passport_date_from")),
                "passport_date_expire": parse_date_ddmmyyyy(item.get("passport_date_expire")),
                "passport_code_place": safe_str(item.get("passport_code_place")),
                "passport_type": safe_str(item.get("passport_type")),
                "num_file": safe_int(item.get("num_file")),
                "photo": safe_str(item.get("photo")),
                
                # System & Audit
                "user_id": safe_str(item.get("user_id")),
                "raw_user_id": safe_int(item.get("raw_user_id")),
                "date_trigger_user": parse_date_ddmmyyyy(item.get("date_trigger_user")),
                "date_created": parse_date_ddmmyyyy(item.get("date_created")),
                "date_updated": parse_date_ddmmyyyy(item.get("date_updated")),
                "created_by_id": safe_str(item.get("created_by_id")),
                "updated_by_id": safe_str(item.get("updated_by_id")),
                
                # Salary
                "salary_real": safe_float(item.get("salary_real")),
                "date_change_salary": parse_date_ddmmyyyy(item.get("date_change_salary")),
                "level_id": safe_str(item.get("level_id")),
                "coefficient": safe_float(item.get("coefficient")),
                "salary_area_id": safe_str(item.get("salary_area_id")),
                "salary_min_area": safe_float(item.get("salary_min_area")),
                
                # Other
                "desc": safe_str(item.get("desc")),
                "level_academic": safe_str(item.get("level_academic")),
                "level_school": safe_str(item.get("level_school")),
                "job_bank_id": safe_str(item.get("job_bank_id")),
                "gps_location_ids": safe_str(item.get("gps_location_ids")),
                "labor_group": safe_str(item.get("labor_group")),
                "is_econtract": safe_str(item.get("is_econtract")),
                "live_manager_id": safe_str(item.get("live_manager_id")),
                "certificate_status": safe_str(item.get("certificate_status")),
                "date_created_certificate": parse_date_ddmmyyyy(item.get("date_created_certificate")),
                "certificate": safe_str(item.get("certificate")),
                "work_place": safe_str(item.get("work_place")),
                "date_leave_from": parse_date_ddmmyyyy(item.get("date_leave_from")),
                "date_leave_to": parse_date_ddmmyyyy(item.get("date_leave_to")),
                "due_date": parse_date_ddmmyyyy(item.get("due_date")),
                
                # Raw data backup - convert to JSON string for BigQuery JSON type
                "raw_data": json.dumps(item, ensure_ascii=False, default=str),
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
