"""
Manual Local Sync for OneOffice Data.

Hàm này cho phép chạy thủ công pipeline OneOffice từ local:
1. Extract dữ liệu từ 1Office API
2. Upload raw JSON lên GCS (backup)
3. Load vào BigQuery bằng streaming insert

Giống với daily sync job (chạy lúc 1h sáng), nhưng chạy thủ công từ local.
Sử dụng OneOfficeLoader để đảm bảo có GCS backup trước khi load vào BigQuery.

Sử dụng khi muốn chạy thủ công thay vì đợi scheduled job (1h sáng).
"""

from datetime import date
from typing import Dict, Any
from src.features.one_office.components.extractor import OneOfficeExtractor
from src.features.one_office.components.loader import OneOfficeLoader
from src.shared.logging import get_logger

logger = get_logger(__name__)


def manual_sync_to_bigquery(snapshot_date: date = None) -> Dict[str, Any]:
    """
    Chạy thủ công pipeline OneOffice: Extract -> GCS Backup -> BigQuery.
    
    Flow giống với daily sync job:
    1. Extract dữ liệu từ 1Office API
    2. Upload raw JSON lên GCS (backup) - tự động bởi OneOfficeLoader
    3. Transform và load vào BigQuery bằng streaming insert
    
    Args:
        snapshot_date: Ngày để snapshot (mặc định: hôm nay)
                      Note: OneOfficeLoader sẽ dùng date.today() bất kể tham số này
                      (có thể cần update loader để support custom date trong tương lai)
        
    Returns:
        Dict chứa kết quả thực thi với các keys:
        - status: "success" hoặc "error"
        - extracted: Số lượng records đã extract
        - bq_table: Tên bảng BigQuery đã load
        - loaded: Số lượng rows đã load vào BigQuery
        - error: Thông báo lỗi (nếu có)
    """
    if snapshot_date is None:
        snapshot_date = date.today()
    
    logger.info(f"Starting manual sync for date: {snapshot_date.isoformat()}")
    
    try:
        # Step 1: Extract từ 1Office API
        logger.info("Step 1: Extracting data from 1Office API...")
        extractor = OneOfficeExtractor()
        data = extractor.fetch_all_personnel()
        count_extracted = len(data)
        logger.info(f"Extracted {count_extracted} personnel records")
        
        if count_extracted == 0:
            return {
                "status": "success",
                "extracted": 0,
                "bq_table": "",
                "loaded": 0,
                "message": "No data to sync"
            }
        
        # Step 2: Load vào BigQuery (OneOfficeLoader sẽ tự động upload raw JSON lên GCS trước)
        logger.info("Step 2: Loading to BigQuery (with GCS backup)...")
        loader = OneOfficeLoader()
        count_loaded = loader.load_snapshots(data)
        
        return {
            "status": "success",
            "extracted": count_extracted,
            "bq_table": loader.table_id,
            "loaded": count_loaded
        }
        
    except Exception as e:
        logger.error(f"Manual sync failed: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "extracted": 0,
            "bq_table": "",
            "loaded": 0
        }


if __name__ == "__main__":
    """
    Chạy trực tiếp từ command line:
    
    python -m src.features.one_office.notebooks.manual_local_sync
    
    Hoặc với date cụ thể:
    python -m src.features.one_office.notebooks.manual_local_sync --date 2024-01-15
    """
    import sys
    from datetime import datetime
    
    snapshot_date = None
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--date" and len(sys.argv) > 2:
            snapshot_date = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
        elif sys.argv[1] in ["-h", "--help"]:
            print(__doc__)
            sys.exit(0)
    
    result = manual_sync_to_bigquery(snapshot_date=snapshot_date)
    
    print("\n" + "="*60)
    print("Manual Sync Result:")
    print("="*60)
    for key, value in result.items():
        print(f"{key}: {value}")
    print("="*60)
    
    if result["status"] == "error":
        sys.exit(1)
