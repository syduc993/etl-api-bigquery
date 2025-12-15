# Nhanh.vn Bills Feature

Module này xử lý việc ETL (Extract, Transform, Load) dữ liệu hóa đơn (Bills) từ Nhanh.vn API vào BigQuery.

## Cấu trúc thư mục

- **`components/`**: Chứa logic chính của feature.
    - `extractor.py`: Logic gọi API `/api/bill/search` và tách products.
    - `loader.py`: Logic flatten nested structures, tải data lên GCS và load trực tiếp vào fact tables.
    - `config.py`: Cấu hình riêng cho feature bills.
    - `types.py`: Định nghĩa các models/schemas.

- **`scripts/`**: Các script hỗ trợ vận hành và kiểm tra dữ liệu.
    - `check_fact_tables_data.py`: Kiểm tra số lượng dòng trong Fact Tables.
    - `check_date_products.py`: Kiểm tra consistency của dữ liệu bill-product.
    - `diagnose_missing_data.py`: Chẩn đoán vấn đề thiếu dữ liệu.

- **`docs/`**: Tài liệu kỹ thuật tham khảo.
    - `BILL_LIST_API.md`: Tài liệu tham khảo về API response của Nhanh.

- **`sql/`**: Các câu script SQL (nếu có) dùng cho transformation.

## Usage

### Chạy Daily Sync
Script chạy hàng ngày để lấy data ngày hôm qua (T-1):
```bash
python src/features/nhanh/bills/daily_sync.py
```

### Chạy Extract thủ công (theo Range)
Sử dụng `pipeline.py` (cần viết script wrapper hoặc dùng python shell):
```python
from src.features.nhanh.bills.pipeline import BillPipeline
from datetime import datetime

pipeline = BillPipeline()
pipeline.run_full_pipeline(
    from_date=datetime(2025, 1, 1),
    to_date=datetime(2025, 1, 31)
)
```
