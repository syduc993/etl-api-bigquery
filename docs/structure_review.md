# Cấu trúc Project ETL API BigQuery

Tài liệu này rà soát cấu trúc thư mục hiện tại và mô tả tác dụng của từng file trong dự án.

## 1. Tổng quan Architecture

Dự án tuân theo kiến trúc Modern Data Stack với các layer:
- **Bronze**: Raw data từ API (lưu trữ GCS/Parquet).
- **Silver**: Cleaned & Transformed data (BigQuery).
- **Gold**: Aggregated business data (BigQuery).

Code được tổ chức theo hướng **Feature-based** (chia theo platform/module) kết hợp huy động qua **Registry Pattern**.

## 2. Cấu trúc thư mục

```
src/
├── config.py                   # Cấu hình chung (Environment variables, Global constants)
├── features/                   # Chứa logic chình cho từng Platform
│   ├── nhanh/                  # Platform Nhanh.vn
│   │   ├── bills/              # Feature Bills (Đơn hàng)
│   │   │   ├── config.py       # Cấu hình riêng cho Bills
│   │   │   ├── daily_sync.py   # Script chạy đồng bộ hàng ngày
│   │   │   ├── extract.py      # Script chạy extract thủ công (CLI)
│   │   │   ├── extractor.py    # Logic gọi API Nhanh.vn
│   │   │   ├── loader.py       # Logic upload lên GCS/BigQuery
│   │   │   ├── pipeline.py     # Class BillPipeline (Orchestrator riêng cho Bills)
│   │   │   ├── transformer.py  # Logic xử lý/làm sạch dữ liệu
│   │   │   └── types.py        # Định nghĩa các Data Class/Type hint
│   │   └── ...
│   └── facebook/               # (Placeholder) Platform Facebook
├── loaders/                    # Các module chịu trách nhiệm Load data
│   └── watermark.py            # Quản lý trạng thái incremental sync
├── pipeline/                   # Core Pipeline Logic
│   └── extraction.py           # Generic Extraction Pipeline (dùng Registry để gọi feature extractor)
├── scripts/                    # Các script vận hành/maintenance
│   ├── check_*.py              # Các script kiểm tra dữ liệu/hệ thống
│   ├── cleanup_*.py            # Các script dọn dẹp
│   ├── setup_*.py              # Các script khởi tạo infra/trigger
│   └── rerun_extraction.py     # Script chạy lại extraction
└── shared/                     # Utilities dùng chung
    ├── gcs/                    # GCS Client wrapper
    ├── nhanh/                  # Nhanh API Client (Base)
    ├── parquet/                # Parquet serialization helpers
    ├── logging.py              # Cấu hình logging chuẩn
    └── exceptions.py           # Custom exceptions
```

## 3. Chi tiết tác dụng từng file (Key Files)

### `src/features/nhanh/bills/`
Đây là ví dụ điển hình cho một "Feature module".
- **`extractor.py`**: Chứa class `BillExtractor`. Logic chính để gọi API `/api/bill/search`, xử lý pagination, tách bill và products.
- **`transformer.py`**: Chứa class `BillTransformer`. Logic để biến đổi dữ liệu thô (JSON) thành dạng phẳng (flat) hoặc format phù hợp với BigQuery schema.
- **`loader.py`**: Chứa class `BillLoader`. Logic tạo External Table trên BigQuery trỏ về GCS, hoặc load data vào bảng.
- **`pipeline.py`**: Chứa `BillPipeline`. Class này đóng gói 3 bước Extract -> Transform -> Load thành các method như `run_extract_load`, `run_full_pipeline`.
- **`daily_sync.py`**: Script entry-point để Cloud Scheduler hoặc Cron job gọi vào. Nó khởi tạo `BillPipeline` và chạy.

### `src/pipeline/`
- **`extraction.py`**: Module này định nghĩa hàm `extract_entity` và `FEATURE_REGISTRY`. 
    - **Tác dụng**: Đây là lớp trừu tượng hóa (Abstraction layer). Thay vì gọi trực tiếp `BillExtractor`, code bên ngoài có thể gọi `extract_entity('nhanh', 'bills', ...)` và module này sẽ tự tìm class tương ứng trong Registry.
    - **Note**: File này có logic handle đặc biệt cho `nhanh.bills` (hàm `_extract_nhanh_bills`), cho thấy sự thiếu nhất quán nhẹ trong abstraction (leaky abstraction).

### `src/shared/`
- **`nhanh/client.py`**: `NhanhApiClient`. Base class xử lý việc authentication, rate limiting, retry logic khi gọi Nhanh API. Các feature extractor sẽ dùng class này.
- **`gcs/loader.py`**: `GCSLoader`. Wrapper để upload file lên Google Cloud Storage, hỗ trợ partitioning (chia thư mục theo ngày/tháng).

## 4. Nhận xét sơ bộ

1.  **Sự chồng chéo (Duplication) trong Orchestration**:
    - Có 2 cách để chạy pipeline: 
        1. Dùng `src/pipeline/extraction.py` (Generic approaches).
        2. Dùng `src/features/nhanh/bills/pipeline.py` (Specific approach).
    - Logic "chia nhỏ ngày" (splitting date range) xuất hiện cả ở `pipeline/extraction.py` và `features/nhanh/bills/pipeline.py`.

2.  **Cấu trúc thư mục**: 
    - Khá rõ ràng và dễ mở rộng. Việc tách `features` thành các folder riêng biệt giúp cô lập logic của từng đối tượng.
    - `shared` chứa đúng các thành phần dùng chung.

3.  **Điểm cần cải thiện (Refactor Idea)**:
    - Nên quy hoạch về một luồng duy nhất. Nếu đã dùng `BillPipeline` class, thì `src/pipeline/extraction.py` nên cực kỳ mỏng hoặc bị loại bỏ để tránh maintenance 2 chỗ.
    - Logic `_extract_nhanh_bills` đang bị hardcode trong `src/pipeline/extraction.py`, nên đẩy hết về `BillExtractor` hoặc `BillPipeline`.

---
Sau khi bạn xem qua danh sách này, chúng ta có thể thảo luận chi tiết hơn về cách sắp xếp lại (refactor) để code gọn gàng hơn.
