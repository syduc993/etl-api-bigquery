# Nhanh.vn to Google Cloud Lakehouse ETL

ETL pipeline để extract data từ Nhanh.vn API và load vào Google Cloud Lakehouse architecture theo Medallion Architecture (Bronze, Silver, Gold).

**Đặc biệt:** Pipeline được thiết kế với **Registry Pattern** và **Plugin Architecture** để dễ dàng mở rộng cho:
- ✅ Nhiều endpoints của Nhanh API (bills, products, customers, orders, depots, users, suppliers, và nhiều hơn nữa)
- ✅ Nhiều platforms khác (Facebook, TikTok, 1Offices, và các platforms khác)

## Kiến trúc

- **Bronze Layer**: Raw JSON data được lưu trong GCS với partitioning theo platform/entity
- **Silver Layer**: Data đã được clean và transform trong BigQuery
- **Gold Layer**: Business aggregates và curated data trong BigQuery

## Tính năng

### Core Features
- ✅ Rate limiting với token bucket algorithm
- ✅ Pagination handling (hỗ trợ next là object/array)
- ✅ Tự động chia date range 31 ngày cho bills
- ✅ Incremental extraction với watermark tracking
- ✅ GCS partitioning (month hoặc day level)
- ✅ Error handling và retry logic
- ✅ Structured logging

### Extensibility Features
- ✅ **Registry Pattern**: Dễ dàng thêm extractors mới
- ✅ **Multi-platform support**: Nhanh, Facebook, TikTok, 1Offices (templates)
- ✅ **Config-driven endpoints**: Thêm endpoints mà không cần sửa code nhiều
- ✅ **Plugin architecture**: Mỗi platform độc lập

### Data Processing
- ✅ SQL transformations cho Silver layer
- ✅ Business aggregations cho Gold layer
- ✅ Monitoring và alerting
- ✅ Cloud Scheduler automation

## Setup

### 1. GCP Infrastructure

Infrastructure đã được tạo:
- GCS Buckets: `sync-nhanhvn-project-bronze`, `sync-nhanhvn-project-silver`
- BigQuery Datasets: `bronze`, `silver`, `gold`

### 2. Secrets Management

Lưu Nhanh API credentials trong GCP Secret Manager:

```bash
cd infrastructure/scripts
chmod +x setup-secrets.sh
./setup-secrets.sh
```

Hoặc xem hướng dẫn chi tiết trong `SETUP_GUIDE.md`

### 3. Environment Variables

Copy `.env.example` sang `.env` và cấu hình:

```bash
cp .env.example .env
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

## Sử dụng

### Basic Usage

**Extract từ một platform và entity:**
```bash
python src/main.py --platform nhanh --entity bills
```

**Extract tất cả entities của một platform:**
```bash
python src/main.py --platform nhanh --entity all
```

**Extract từ nhiều platforms:**
```bash
# Chạy orchestrator với tất cả platforms
python src/orchestrator.py --phase bronze
```

### Advanced Usage

**Full pipeline (Bronze → Silver → Gold):**
```bash
python src/orchestrator.py --phase all
```

**Chạy từng phase:**
```bash
# Bronze extraction
python src/orchestrator.py --phase bronze

# Silver transformation
python src/orchestrator.py --phase silver

# Gold aggregation
python src/orchestrator.py --phase gold
```

**Full sync (không incremental):**
```bash
python src/main.py --platform nhanh --entity all --full-sync
```

## Cấu trúc Project

```
.
├── src/
│   ├── config.py              # Quản lý cấu hình
│   ├── main.py                 # Entry point cho Bronze extraction (multi-platform)
│   ├── transform_silver.py     # Entry point cho Silver transformation
│   ├── transform_gold.py       # Entry point cho Gold aggregation
│   ├── orchestrator.py         # Orchestrator cho toàn bộ pipeline
│   ├── extractors/             # Extractors module
│   │   ├── __init__.py         # Main module với registry
│   │   ├── registry.py         # Registry pattern
│   │   ├── config.py           # Endpoint và platform configs
│   │   ├── nhanh/              # Nhanh API extractors
│   │   │   ├── __init__.py     # Đăng ký Nhanh extractors
│   │   │   ├── base.py         # NhanhApiClient
│   │   │   ├── bill.py
│   │   │   ├── product.py
│   │   │   ├── customer.py
│   │   │   ├── order.py
│   │   │   ├── business.py
│   │   │   └── ...             # Thêm extractors mới ở đây
│   │   ├── facebook/            # Facebook API extractors (template)
│   │   ├── tiktok/              # TikTok API extractors (template)
│   │   └── oneoffices/          # 1Offices API extractors (template)
│   ├── loaders/                # Data loaders
│   ├── transformers/           # Data transformers
│   ├── monitoring/             # Monitoring và metrics
│   └── utils/                  # Utilities
├── sql/                        # SQL transformation scripts
│   ├── bronze/                 # Bronze layer schemas
│   ├── silver/                 # Silver transformations
│   └── gold/                   # Gold aggregations
├── infrastructure/             # Infrastructure as Code
└── docs/                       # Documentation
```

## Mở rộng Pipeline

### Thêm Endpoint mới cho Nhanh API

Xem hướng dẫn chi tiết trong `docs/EXTENSIBILITY_GUIDE.md`

**Ví dụ nhanh:**
1. Thêm config trong `src/extractors/config.py`
2. Tạo extractor class trong `src/extractors/nhanh/`
3. Đăng ký trong `src/extractors/nhanh/__init__.py`
4. Done! Sử dụng ngay

### Thêm Platform mới

Xem hướng dẫn chi tiết trong `docs/EXTENSIBILITY_GUIDE.md`

**Ví dụ nhanh:**
1. Tạo platform directory: `src/extractors/facebook/`
2. Implement base client
3. Tạo extractors
4. Đăng ký trong `__init__.py`
5. Done! Platform mới sẵn sàng

## API Constraints

- **31-Day Limit**: Bills API chỉ hỗ trợ date range tối đa 31 ngày
- **Rate Limit**: 150 requests per 30 seconds (per appId + businessId + URL)
- **Pagination**: `paginator.next` là object/array, không phải string

## Workflow

### Bronze Layer (Extraction)

1. Extract data từ APIs (Nhanh, Facebook, TikTok, etc.)
2. Upload JSON lên GCS với partitioning: `{platform}/{entity}/year=.../month=.../`
3. Track watermark cho incremental extraction

### Silver Layer (Transformation)

1. Tạo BigQuery External Tables pointing đến GCS
2. Chạy SQL transformations:
   - Type casting
   - Deduplication
   - Flatten nested JSON
   - Data validation
3. Lưu kết quả vào Silver tables

### Gold Layer (Aggregation)

1. Tạo business aggregates:
   - Daily revenue summary
   - Customer lifetime value
   - Product sales metrics
   - Inventory analytics
2. Tạo materialized views cho performance
3. Partition và cluster tables

## Documentation

- `SETUP_GUIDE.md`: Hướng dẫn setup chi tiết
- `docs/EXTENSIBILITY_GUIDE.md`: Hướng dẫn mở rộng pipeline
- `docs/ARCHITECTURE.md`: Kiến trúc chi tiết
- `docs/SECURITY_AUDIT.md`: Security audit checklist
- `docs/COST_OPTIMIZATION.md`: Cost optimization guide
- `docs/PRODUCTION_READINESS.md`: Production readiness checklist
- `docs/RUNBOOK.md`: Operations và troubleshooting guide
- `docs/IMPLEMENTATION_SUMMARY.md`: Tổng kết implementation
- `etl-plan/PROJECT_PLAN.md`: Kế hoạch dự án đầy đủ

## Extensibility

Pipeline được thiết kế để dễ dàng mở rộng:

### Hiện tại đã implement
- ✅ Nhanh API: bills, products, customers, orders, depots, users, suppliers
- ✅ Registry pattern cho quản lý extractors
- ✅ Config-driven endpoints
- ✅ Multi-platform structure (templates)

### Sẵn sàng để mở rộng
- 🔄 Thêm các endpoints Nhanh còn lại (categories, brands, promotions, accounting, etc.)
- 🔄 Implement Facebook API
- 🔄 Implement TikTok API
- 🔄 Implement 1Offices API

Xem `docs/EXTENSIBILITY_GUIDE.md` để biết cách thêm endpoints và platforms mới.

## Production Readiness

Pipeline đã sẵn sàng cho production với:

- ✅ Load testing scripts
- ✅ Error recovery testing
- ✅ Security audit checklist
- ✅ Cost optimization review
- ✅ Complete documentation
- ✅ Operations runbook

Xem chi tiết trong:
- `docs/PRODUCTION_READINESS.md` - Production readiness checklist
- `docs/SECURITY_AUDIT.md` - Security audit
- `docs/COST_OPTIMIZATION.md` - Cost optimization
- `docs/RUNBOOK.md` - Operations runbook
- `docs/IMPLEMENTATION_SUMMARY.md` - Tổng kết implementation
