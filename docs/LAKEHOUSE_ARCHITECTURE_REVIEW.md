# Đánh Giá Kiến Trúc Data Lake House

**Ngày đánh giá:** 2025-12-17  
**Project:** sync-nhanhvn-project  
**Region:** asia-southeast1

## Tổng Quan Kiến Trúc

### Pattern Đang Sử Dụng
**Bronze → Gold (Bỏ qua Silver Layer)**

```
API → Extractors → GCS (Bronze/Parquet) → BigQuery (Gold/Fact Tables)
                                          ↓
                            External Tables (bronze.*_raw) → Optional views
```

## 1. Bronze Layer (GCS - Raw Data Storage)

### ✅ Điểm Mạnh

**Storage:**
- **Bucket:** `sync-nhanhvn-project` (location: ASIA-SOUTHEAST1)
- **Format:** Parquet files (tối ưu cho analytics)
- **Partitioning:** Hive-style partitioning (`year=YYYY/month=MM/day=DD/`)

**Structure:**
```
gs://sync-nhanhvn-project/
├── nhanh/
│   ├── bills/year=2025/month=11/.../*.parquet
│   └── bill_products/year=2025/month=11/.../*.parquet
└── oneoffice/
    └── hr_profile/year=2025/month=12/day=13/.../*.json.gz
```

**Đánh Giá:**
- ✅ Partitioning strategy phù hợp (day-level cho query performance)
- ✅ Format Parquet tối ưu cho BigQuery
- ✅ Location đúng region (asia-southeast1) - giảm latency

### ⚠️ Lưu Ý

- **Metadata files:** Có thư mục `_metadata/` trong GCS (có thể cleanup nếu không cần)
- **File organization:** Một số partitions có thể có nhiều files (overwrite pattern đảm bảo chỉ 1 file mới nhất)

## 2. External Tables (Bronze Views)

### ✅ Điểm Mạnh

**Implementation:**
- **Dataset:** `bronze`
- **Tables:**
  - `bronze.nhanh_bills_raw` (EXTERNAL)
  - `bronze.nhanh_bill_products_raw` (EXTERNAL)

**Configuration:**
```sql
-- nhanh_bills_raw
Source: gs://sync-nhanhvn-project/nhanh/bills/*.parquet
Format: PARQUET

-- nhanh_bill_products_raw  
Source: gs://sync-nhanhvn-project/nhanh/bill_products/*.parquet
Format: PARQUET
```

**Đánh Giá:**
- ✅ External tables là optional views để query trực tiếp GCS nếu cần
- ✅ Format Parquet được hỗ trợ tốt bởi BigQuery
- ✅ Không phải Silver layer - đúng với kiến trúc đơn giản (Bronze → Gold)

### 📝 Ghi Chú

- External tables trỏ về **cùng Bronze bucket** - không phải Silver layer
- Có thể dùng để query raw data mà không cần load vào native tables
- Không có transformations - chỉ là views trực tiếp đến GCS

## 3. Gold Layer (BigQuery Native Tables)

### ✅ Điểm Mạnh

**Dataset: `nhanhVN`** (Fact Tables)

**Tables Chính:**

1. **`fact_sales_bills_v3_0`**
   - **Rows:** 472,207 records
   - **Partitioning:** DAY by `date` field
   - **Schema:** Flattened structure (customer, payment, sale fields đã flatten)
   - **Coverage:** 46 partitions (2025-11-01 → 2025-12-16)
   - **Size:** ~98 MB

2. **`fact_sales_bills_product_v3_0`**
   - **Rows:** 2,443,620 records  
   - **Partitioning:** DAY by `DATE(extraction_timestamp)`
   - **Schema:** Flattened product structure (vat fields đã flatten)
   - **Size:** ~151 MB

**Dataset: `oneoffice`**

3. **`hr_profile_daily_snapshot`**
   - **Partitioning:** DAY by `snapshot_date`
   - **Pattern:** Daily snapshots (streaming insert)

**Đánh Giá:**
- ✅ **Partitioning đúng:** Day-level partitioning cho cả 2 fact tables
- ✅ **Schema hợp lý:** Flattened structure phù hợp cho analytics
- ✅ **Data volume:** Hợp lý cho batch processing
- ✅ **Coverage:** 46 ngày data (Nov 1 - Dec 16) - consistent

### ⚠️ Cần Kiểm Tra

- **fact_sales_bills_product_v3_0:** Query partition coverage không trả về kết quả - cần verify extraction_timestamp có data không
- **Data freshness:** Latest partition là 2025-12-16 - cần đảm bảo pipeline chạy daily

## 4. Data Flow Analysis

### Current Flow

```
1. API Extraction (Nhanh API, 1Office API)
   ↓
2. Flatten nested structures (trong Loader)
   ↓
3. Upload Parquet lên GCS (Bronze backup)
   ↓
4. LoadJob vào BigQuery Gold Tables (fact_sales_bills_v3_0, fact_sales_bills_product_v3_0)
   ↓
5. External Tables (bronze.*_raw) - Optional views
```

### ✅ Điểm Mạnh

- **Simple & Direct:** Bronze → Gold trực tiếp (không có Silver layer phức tạp)
- **Backup strategy:** GCS backup trước khi load BigQuery (đảm bảo data không mất)
- **Idempotent:** DELETE partition + WRITE_APPEND đảm bảo re-run an toàn
- **Transformations:** Flatten được làm trong Python (loader) - dễ debug và maintain

### 📊 Data Quality Metrics

**Bills Table:**
- Total rows: 472,207
- Distinct dates: 46 partitions
- Date range: 2025-11-01 → 2025-12-16
- Average rows/partition: ~10,266

**Products Table:**
- Total rows: 2,443,620
- Ratio to bills: ~5.2 products per bill (hợp lý)

## 5. Kiến Trúc So Với Best Practices

### ✅ Tuân Thủ

1. **Medallion Pattern (Simplified):**
   - ✅ Bronze layer: Raw data trong GCS (Parquet)
   - ✅ Gold layer: Curated data trong BigQuery (Fact tables)
   - ✅ Bỏ qua Silver layer: Phù hợp với use case đơn giản

2. **Partitioning Strategy:**
   - ✅ Day-level partitioning cho time-series data
   - ✅ Hive-style partitioning trong GCS
   - ✅ BigQuery native partitioning bằng DATE field

3. **Data Format:**
   - ✅ Parquet format (columnar, compressed, schema-aware)
   - ✅ Tương thích tốt với BigQuery

4. **Data Governance:**
   - ✅ External tables trong `bronze` dataset (rõ ràng về purpose)
   - ✅ Fact tables trong `nhanhVN` dataset (business-ready)

### 📝 Khuyến Nghị

1. **Monitoring & Alerts:**
   - ✅ Nên có alerts cho pipeline failures
   - ✅ Nên có data quality checks (row counts, date gaps)
   - ✅ Nên có schema change detection

2. **Documentation:**
   - ✅ Đã document rõ pattern Bronze → Gold (bỏ qua Silver)
   - ✅ External tables là optional views

3. **Performance:**
   - ✅ Partitioning strategy tối ưu cho queries
   - ✅ Có thể thêm clustering nếu cần (ví dụ: cluster by depotId, type)

4. **Cost Optimization:**
   - ✅ Parquet format giảm storage cost
   - ✅ Partitioning giảm query cost (partition pruning)
   - ⚠️ Nên có lifecycle policy cho GCS (move to Nearline/Archive sau X ngày)

## 6. Tổng Kết Đánh Giá

### Điểm Mạnh Tổng Thể

1. ✅ **Kiến trúc đơn giản và hiệu quả:** Bronze → Gold trực tiếp phù hợp với use case
2. ✅ **Partitioning strategy đúng:** Day-level partitioning tối ưu cho analytics
3. ✅ **Data format tối ưu:** Parquet format cho performance và cost
4. ✅ **Data quality:** Coverage đầy đủ (46 partitions), data volume hợp lý
5. ✅ **Backup strategy:** GCS backup trước khi load BigQuery

### Điểm Cần Cải Thiện

1. ⚠️ **fact_sales_bills_product_v3_0:** Cần verify partition coverage query (extraction_timestamp có data không)
2. 📝 **Monitoring:** Nên có alerts và data quality checks tự động
3. 📝 **Lifecycle Management:** Nên có GCS lifecycle policy cho cost optimization
4. 📝 **Documentation:** Có thể thêm data lineage documentation

### Kết Luận

**Kiến trúc hiện tại: 8.5/10**

- ✅ Kiến trúc đơn giản, phù hợp với yêu cầu
- ✅ Implementation đúng best practices (partitioning, format, backup)
- ✅ Data quality tốt, coverage đầy đủ
- ⚠️ Cần thêm monitoring và lifecycle management

**Khuyến nghị:** Giữ nguyên kiến trúc hiện tại, chỉ cần thêm monitoring và lifecycle policies.

