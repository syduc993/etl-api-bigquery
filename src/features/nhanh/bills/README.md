# Nhanh.vn Bills Feature

Module này xử lý việc ETL (Extract, Transform, Load) dữ liệu hóa đơn (Bills) từ Nhanh.vn API vào BigQuery.

## Cấu trúc thư mục

- **`components/`**: Chứa logic chính của feature.
    - `extractor.py`: Logic gọi API `/api/bill/search` và tách products.
    - `transformer.py`: Logic xử lý data (làm sạch, type casting) trước khi đẩy vào Fact table.
    - `loader.py`: Logic tải data lên GCS và setup External Tables.
    - `config.py`: Cấu hình riêng cho feature bills.
    - `types.py`: Định nghĩa các models/schemas.

- **`scripts/`**: Các script hỗ trợ vận hành và kiểm tra dữ liệu.
    - `repopulate_fact_tables.py`: Chạy lại logic transform từ raw data.
    - `check_bq_data.py`: Kiểm tra số lượng dòng trong BigQuery.
    - `check_date_products.py`: Kiểm tra consistency của dữ liệu bill-product.

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
