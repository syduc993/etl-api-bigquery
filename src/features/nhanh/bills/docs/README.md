# Bills Feature - Nhanh.vn Hóa Đơn

## File Structure

```
src/features/nhanh/bills/
├── __init__.py              # Module exports (BillExtractor, BillLoader, BillPipeline)
├── config.py                # Constants: bill modes, types, API endpoints, pagination
├── types.py                 # Type definitions: BillSchema dataclass với field definitions
│
├── daily_sync.py            # Entry point: Full pipeline (Extract + Load) - dùng cho scheduled jobs
│
├── components/
│   ├── extractor.py         # BillExtractor: Extract bills từ Nhanh API /bill/list
│   ├── loader.py            # BillLoader: Flatten nested structures, upload Parquet lên GCS, load trực tiếp vào fact tables
│   └── pipeline.py          # BillPipeline: Orchestrate Extract → Load flow (flatten integrated in loader)
│
├── sql/
│   ├── schema_clean.sql     # Schema definition cho fact tables (fact_sales_bills_v3_0, fact_sales_bills_product_v3_0)
│   ├── query_flatten.sql    # SQL transformation: Flatten nested structures từ External Tables
│   └── bronze_products_schema.sql  # Schema cho bronze.bill_products_raw External Table
│
├── docs/
│   ├── README.md            # Documentation này
│   └── nhanhVN.md           # Nhanh API documentation reference
│
└── tests/
    ├── __init__.py          # Test module
    └── test_extractor.py    # Unit tests cho BillExtractor
```

### File Descriptions

#### Entry Points (Scripts)
- **`daily_sync.py`**: Full pipeline script chạy Extract → Load (flatten integrated). Dùng cho scheduled jobs (GitHub Actions, Cloud Run). Luôn chạy incremental extraction cho ngày hôm qua (T-1).

#### Core Modules
- **`components/extractor.py`**: `BillExtractor` class - Extract dữ liệu từ Nhanh API với pagination, date range splitting, incremental support.
- **`components/loader.py`**: `BillLoader` class - Flatten nested structures trong Python, upload bills và bill_products lên GCS dưới dạng Parquet, và load trực tiếp vào fact tables (BigQuery native tables).
- **`components/pipeline.py`**: `BillPipeline` class - Orchestrate toàn bộ ETL flow, kết hợp Extractor + Loader (flatten integrated).

#### Configuration & Types
- **`config.py`**: Constants cho bills feature: bill modes (RETAIL=2, WHOLESALE=6), types (IMPORT=1, EXPORT=2), API endpoints, pagination settings.
- **`types.py`**: `BillSchema` dataclass - Type definitions và schema cho bills entity, field definitions.
- **`__init__.py`**: Module exports - Export các classes chính để import từ package.

#### SQL Files
- **`sql/schema_clean.sql`**: Tạo schema cho fact tables (`fact_sales_bills_v3_0`, `fact_sales_bills_product_v3_0`) với partitioning và clustering.
- **`sql/query_flatten.sql`**: SQL transformation reference (deprecated - flatten now done in Python). Giữ lại để reference nếu cần rollback logic.
- **`sql/bronze_products_schema.sql`**: Schema definition cho `bronze.nhanh_bill_products_raw` External Table (reference, không dùng trực tiếp).

#### Documentation & Tests
- **`docs/README.md`**: Documentation này - Hướng dẫn sử dụng feature.
- **`docs/nhanhVN.md`**: Nhanh API documentation reference - API specs, request/response examples.
- **`tests/test_extractor.py`**: Unit tests cho `BillExtractor` class.

---

## Overview

Feature này chịu trách nhiệm Extract, Transform và Load dữ liệu hóa đơn (`bills`) từ Nhanh.vn API vào BigQuery.

**Data Flow:**
```
Nhanh API → Flatten (Python) → GCS (Parquet) → BigQuery Fact Tables (Native)
```

**Output Tables:**
- `sync-nhanhvn-project.nhanhVN.fact_sales_bills_v3_0` (Native Table - partitioned by date)
- `sync-nhanhvn-project.nhanhVN.fact_sales_bills_product_v3_0` (Native Table - partitioned by extraction_timestamp)

**Note:** Bronze external tables (`bronze.nhanh_bills_raw`, `bronze.nhanh_bill_products_raw`) vẫn được giữ lại để backup (chỉ metadata, không tốn storage).

---

## Entry Points (Scripts)

### `daily_sync.py` - Full Pipeline (Recommended)
Chạy toàn bộ pipeline: Extract → Flatten → Load. Script này được dùng cho scheduled jobs (GitHub Actions, Cloud Run Jobs).

**Usage:**
```bash
python src/features/nhanh/bills/daily_sync.py
```

**Chức năng:**
1. **Extract:** Extract bills từ Nhanh API `/bill/list`
2. **Flatten:** Flatten nested structures (customer, payment, products) trong Python
3. **Load:** Upload Parquet files lên GCS và load trực tiếp vào fact tables (BigQuery native tables)

**Lưu ý:** Script này luôn chạy cho ngày hôm qua (T-1) theo timezone VN (UTC+7).

---

## Core Modules

### `extractor.py` - BillExtractor
Extract dữ liệu từ Nhanh API với các tính năng:
- Tự động chia date range thành chunks 31 ngày (do API giới hạn)
- Hỗ trợ incremental extraction dựa trên `updatedAt`
- Hỗ trợ filters: `modes`, `type`, `customerId`, `fromDate`/`toDate`
- Tách bills và bill_products thành 2 datasets riêng

**API Endpoint:** `/bill/list`

---

### `loader.py` - BillLoader
Flatten nested structures và load dữ liệu vào BigQuery fact tables:
- **Flatten:** Flatten nested structures (customer, payment, sale, products) trong Python
- **Upload:** Upload bills và bill_products lên GCS dưới dạng Parquet (backup)
- **Load:** Load trực tiếp vào fact tables (BigQuery native tables) với "DELETE partition + WRITE_APPEND" strategy để đảm bảo idempotency
- Partition theo date: `nhanh/bills/year=YYYY/month=MM/day=DD/`
- Tự động tạo schema nếu chưa tồn tại

**Strategy:**
- Bills: DELETE partition data + WRITE_APPEND (idempotent)
- Products: DELETE partition data + WRITE_APPEND (idempotent)

---

### `pipeline.py` - BillPipeline
Orchestrate toàn bộ ETL flow:
- `run_extract_load()` - Extract + Flatten + Load (all in one, flatten integrated in loader)
- `run_full_pipeline()` - Alias cho `run_extract_load()` (backward compatibility)

---

## Data Flow

### 1. Extraction Phase
```
Nhanh API (/bill/list)
    ↓
BillExtractor.extract()
    ↓
Raw JSON data (bills + bill_products)
```

### 2. Flattening & Loading Phase
```
Raw JSON data (nested structures)
    ↓
BillLoader._flatten_bill() / _flatten_bill_product() (Python)
    ↓
Flat records matching BigQuery schema
    ↓
Upload to GCS (Parquet backup)
    ↓
Load to BigQuery (DELETE partition + WRITE_APPEND)
    ↓
Native Tables: nhanhVN.fact_sales_bills_v3_0, nhanhVN.fact_sales_bills_product_v3_0
```

---

## SQL Files

### `sql/schema_clean.sql`
Định nghĩa schema cho fact tables:
- `fact_sales_bills_v3_0`: Partitioned by `date`, clustered by `depotId, type`
- `fact_sales_bills_product_v3_0`: Partitioned by `extraction_timestamp`, clustered by `bill_id, product_id`

### `sql/query_flatten.sql`
**DEPRECATED:** SQL transformation reference (flatten now done in Python).
- Giữ lại để reference nếu cần rollback logic
- Không còn được sử dụng trong pipeline hiện tại

### `sql/bronze_products_schema.sql`
Schema definition cho bronze products (nếu cần).

---

## Configuration

Các cấu hình được định nghĩa trong `src/config.py`:
- `GCP_PROJECT`: `sync-nhanhvn-project`
- `GCP_REGION`: `asia-southeast1`
- `BRONZE_BUCKET`: GCS bucket name
- `BRONZE_DATASET`: `bronze`
- `TARGET_DATASET`: `nhanhVN`
- Nhanh API credentials: `NHANH_API_KEY`, `NHANH_STORE_ID`, etc.

---

## Usage Examples

### Incremental Daily Sync (Recommended)
```bash
# Chạy mỗi ngày để sync data mới (ngày hôm qua T-1)
python src/features/nhanh/bills/daily_sync.py
```

### Full Historical Sync
```python
# Sử dụng pipeline với date range
from src.features.nhanh.bills.pipeline import BillPipeline
from datetime import datetime

pipeline = BillPipeline()
pipeline.run_extract_load(
    from_date=datetime(2024, 1, 1),
    to_date=datetime(2024, 1, 31)
)
```

---

## Watermark Tracking

Feature sử dụng watermark để track lần extraction cuối cùng:
- Table: `bronze.extraction_watermarks`
- Key: `nhanh_bills`
- Value: Last successful extraction timestamp
- Incremental extraction tự động lấy từ watermark → hiện tại

---

## Notes

- **API Limitation:** Nhanh API giới hạn date range tối đa 31 ngày. Extractor tự động chia thành chunks.
- **Data Format:** Data được flatten trong Python, sau đó lưu dưới dạng Parquet trong GCS (backup) và load trực tiếp vào BigQuery native tables.
- **Idempotency:** Loader sử dụng "DELETE partition + WRITE_APPEND" strategy để đảm bảo không có duplicate data khi chạy lại.
- **Partitioning:** Fact tables được partition theo date (`fact_sales_bills_v3_0`) hoặc `extraction_timestamp` (`fact_sales_bills_product_v3_0`) để tối ưu query và cost.
- **Bronze External Tables:** Vẫn được giữ lại để backup (chỉ metadata, không tốn storage), có thể dùng để query raw data nếu cần.
