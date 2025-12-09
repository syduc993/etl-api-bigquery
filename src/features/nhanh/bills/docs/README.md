# Bills Feature - Nhanh.vn Hóa Đơn

## File Structure

```
src/features/nhanh/bills/
├── __init__.py              # Module exports (BillExtractor, BillTransformer, BillLoader, BillPipeline)
├── config.py                # Constants: bill modes, types, API endpoints, pagination
├── types.py                 # Type definitions: BillSchema dataclass với field definitions
│
├── extract.py               # Entry point: Extract only (Nhanh API → GCS → External Tables)
├── daily_sync.py            # Entry point: Full pipeline (Extract + Transform) - dùng cho scheduled jobs
├── run_transform.py         # Entry point: Transform only (Bronze → Fact Tables)
├── manual_test.py           # Manual testing script cho development/debugging
│
├── extractor.py             # BillExtractor: Extract bills từ Nhanh API /bill/list
├── loader.py                # BillLoader: Upload Parquet lên GCS, setup External Tables
├── transformer.py           # BillTransformer: Transform từ Bronze → Fact Tables
├── pipeline.py              # BillPipeline: Orchestrate E→T→L flow
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
- **`extract.py`**: Extract bills từ Nhanh API và upload lên GCS, setup External Tables. Hỗ trợ incremental hoặc date range extraction.
- **`daily_sync.py`**: Full pipeline script chạy Extract → Load → Transform. Dùng cho scheduled jobs (GitHub Actions, Cloud Run). Luôn chạy incremental extraction.
- **`run_transform.py`**: Chỉ chạy transformation từ Bronze External Tables → Fact Tables. Dùng khi đã có data trong Bronze và chỉ cần transform lại.
- **`manual_test.py`**: Script để test thủ công trong development. Có thể chỉnh sửa date range và test các functions.

#### Core Modules
- **`extractor.py`**: `BillExtractor` class - Extract dữ liệu từ Nhanh API với pagination, date range splitting, incremental support.
- **`loader.py`**: `BillLoader` class - Upload bills và bill_products lên GCS dưới dạng Parquet, setup BigQuery External Tables.
- **`transformer.py`**: `BillTransformer` class - Transform data từ Bronze External Tables → Fact Native Tables bằng SQL.
- **`pipeline.py`**: `BillPipeline` class - Orchestrate toàn bộ ETL flow, kết hợp Extractor + Loader + Transformer.

#### Configuration & Types
- **`config.py`**: Constants cho bills feature: bill modes (RETAIL=2, WHOLESALE=6), types (IMPORT=1, EXPORT=2), API endpoints, pagination settings.
- **`types.py`**: `BillSchema` dataclass - Type definitions và schema cho bills entity, field definitions.
- **`__init__.py`**: Module exports - Export các classes chính để import từ package.

#### SQL Files
- **`sql/schema_clean.sql`**: Tạo schema cho fact tables (`fact_sales_bills_v3_0`, `fact_sales_bills_product_v3_0`) với partitioning và clustering.
- **`sql/query_flatten.sql`**: SQL transformation để flatten nested structures (customer, payment, products) từ External Tables và MERGE/INSERT vào fact tables.
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
Nhanh API → GCS (Bronze) → BigQuery External Tables → Fact Tables (Native)
```

**Output Tables:**
- `sync-nhanhvn-project.bronze.nhanh_bills_raw` (External Table - đọc từ GCS)
- `sync-nhanhvn-project.bronze.nhanh_bill_products_raw` (External Table - đọc từ GCS)
- `sync-nhanhvn-project.nhanhVN.fact_sales_bills_v3_0` (Native Table - partitioned by date)
- `sync-nhanhvn-project.nhanhVN.fact_sales_bills_product_v3_0` (Native Table - partitioned by extraction_timestamp)

---

## Entry Points (Scripts)

### 1. `extract.py` - Extract Only
Extract dữ liệu từ Nhanh API và upload lên GCS (Bronze layer), sau đó setup BigQuery External Tables.

**Usage:**
```bash
# Incremental extraction (tự động từ watermark)
python src/features/nhanh/bills/extract.py

# Date range extraction
python src/features/nhanh/bills/extract.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31
```

**Chức năng:**
- Extract bills từ Nhanh API `/bill/list`
- Upload Parquet files lên GCS với partition theo date
- Setup BigQuery External Tables (`bronze.nhanh_bills_raw`, `bronze.nhanh_bill_products_raw`)
- **Không** transform vào fact tables

---

### 2. `daily_sync.py` - Full Pipeline (Recommended)
Chạy toàn bộ pipeline: Extract → Load → Transform. Script này được dùng cho scheduled jobs (GitHub Actions, Cloud Run Jobs).

**Usage:**
```bash
python src/features/nhanh/bills/daily_sync.py
```

**Chức năng:**
1. **Step 1:** Incremental extraction từ Nhanh API → GCS
2. **Step 2:** Setup BigQuery External Tables
3. **Step 3:** Transform từ Bronze External Tables → Fact Tables (Native)

**Lưu ý:** Script này luôn chạy incremental extraction (không nhận date range arguments).

---

### 3. `run_transform.py` - Transform Only
Chỉ chạy transformation từ Bronze External Tables → Fact Tables. Dùng khi đã có data trong Bronze và chỉ cần transform lại.

**Usage:**
```bash
python src/features/nhanh/bills/run_transform.py
```

**Chức năng:**
- Đọc từ `bronze.nhanh_bills_raw` và `bronze.nhanh_bill_products_raw`
- Flatten nested structures (customer, payment, products)
- MERGE vào `nhanhVN.fact_sales_bills_v3_0` và INSERT vào `nhanhVN.fact_sales_bills_product_v3_0`

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
Upload dữ liệu lên GCS và setup BigQuery External Tables:
- Upload bills và bill_products lên GCS dưới dạng Parquet
- Partition theo date: `nhanh/bills/year=YYYY/month=MM/day=DD/`
- Setup External Tables trỏ đến GCS paths
- Hỗ trợ overwrite partition khi cần

---

### `transformer.py` - BillTransformer
Transform dữ liệu từ Bronze (External) → Fact Tables (Native):
- Flatten nested structures (customer, payment, sale, products)
- MERGE strategy cho bills (upsert)
- INSERT strategy cho bill_products (append)
- Tự động tạo schema nếu chưa tồn tại

**SQL Files:**
- `sql/schema_clean.sql` - Tạo fact tables schema
- `sql/query_flatten.sql` - Flatten và insert data

---

### `pipeline.py` - BillPipeline
Orchestrate toàn bộ ETL flow:
- `run_extract_load()` - Extract + Load to GCS
- `run_transform()` - Transform to fact tables
- `run_full_pipeline()` - Full ETL (Extract → Load → Transform)

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

### 2. Loading Phase
```
Raw JSON data
    ↓
BillLoader.load_bills() / load_bill_products()
    ↓
GCS Parquet files (Bronze layer)
    ↓
BigQueryExternalTableSetup.setup_external_table()
    ↓
External Tables: bronze.nhanh_bills_raw, bronze.nhanh_bill_products_raw
```

### 3. Transformation Phase
```
External Tables (Parquet STRUCT columns)
    ↓
BillTransformer.transform_flatten()
    ↓
SQL: query_flatten.sql
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
SQL transformation để flatten data:
- Đọc từ External Tables (Parquet với STRUCT columns)
- Flatten nested objects: `customer.*`, `payment.*`, `sale.*`, `products[]`
- MERGE vào `fact_sales_bills_v3_0` (upsert)
- INSERT vào `fact_sales_bills_product_v3_0` (append)

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
# Chạy mỗi ngày để sync data mới
python src/features/nhanh/bills/daily_sync.py
```

### Full Historical Sync
```bash
# Extract data từ date range
python src/features/nhanh/bills/extract.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31

# Sau đó transform
python src/features/nhanh/bills/run_transform.py
```

### Re-transform Existing Data
```bash
# Nếu đã có data trong Bronze, chỉ cần transform lại
python src/features/nhanh/bills/run_transform.py
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
- **Data Format:** Data được lưu dưới dạng Parquet trong GCS để tối ưu storage và query performance.
- **External vs Native:** Bronze layer dùng External Tables (đọc từ GCS), Fact layer dùng Native Tables (lưu trong BigQuery) để tối ưu query performance.
- **Partitioning:** Fact tables được partition theo date để tối ưu query và cost.
