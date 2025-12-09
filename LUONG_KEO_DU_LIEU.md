# Luồng Kéo Dữ Liệu Nhanh.vn vào BigQuery

Tài liệu này mô tả chi tiết cách hệ thống ETL kéo dữ liệu từ Nhanh API, xử lý và đưa vào BigQuery.

## Tổng Quan Luồng Dữ Liệu

```
Command Line Arguments
    ↓
Feature-specific Entry Point (src/features/nhanh/bills/extract.py)
    ↓
extract_entity() - Xác định platform/entity
    ↓
BillExtractor.extract_with_products()
    ↓
NhanhApiClient.fetch_paginated() - Gọi Nhanh API
    ↓
Tách bills và products
    ↓
GCSLoader.upload_parquet_by_date() - Upload lên GCS
    ↓
BigQueryExternalTableSetup.setup_external_table() - Tạo External Table
    ↓
BigQuery (Query được thực hiện trên External Table)
```

---

## 1. Điểm Vào (Entry Point)

### Cấu Trúc Entry Points

Hệ thống sử dụng **các file entry point riêng cho từng feature** thay vì một file `main.py` chung. Mỗi feature có script riêng trong thư mục `src/scripts/`.

**Các entry points hiện có:**

| File | Feature | Mô tả |
|------|---------|-------|
| `src/features/nhanh/bills/extract.py` | Nhanh Bills | Extract bills từ Nhanh API |
| `src/features/nhanh/bills/daily_sync.py` | Daily Sync | Extract + Transform (dùng cho schedule) |
| `src/features/nhanh/bills/run_transform.py` | Transform Only | Chỉ transform từ Bronze → Fact |

### File: `src/features/nhanh/bills/extract.py` (Entry Point Chính)

**Chức năng:** Entry point riêng cho việc extract bills từ Nhanh.vn API. File này được thiết kế đơn giản và tập trung vào một feature cụ thể.

**Các tham số command line:**

```bash
python src/features/nhanh/bills/extract.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31
```

**Các tham số hiện tại:**

| Tham số | Mô tả | Bắt buộc |
|---------|-------|----------|
| `--from-date` | Ngày bắt đầu (format: YYYY-MM-DD) | Có (nếu dùng date range) |
| `--to-date` | Ngày kết thúc (format: YYYY-MM-DD) | Có (nếu dùng date range) |

**Lưu ý:**
- Nếu **không có** `--from-date` và `--to-date` → Sử dụng **incremental extraction** (tự động lấy từ watermark)
- Nếu **có** cả `--from-date` và `--to-date` → Sử dụng **date range extraction** (full sync trong khoảng thời gian)

**So sánh với `main.py` (cũ):**

`main.py` có nhiều tham số hơn vì nó là entry point chung cho tất cả platforms và entities:
- `--platform`: Chọn platform (nhanh, facebook, tiktok)
- `--entity`: Chọn entity (bills, products, customers)
- `--incremental`: Bật/tắt incremental mode
- `--full-sync`: Force full sync
- `--batch-days`: Số ngày mỗi batch

`extract.py` đơn giản hơn vì:
- `platform` và `entity` đã **hard-code** thành `"nhanh"` và `"bills"`
- `incremental` được **tự động xác định** dựa trên có date range hay không
- Các filter khác (`modes`, `bill_type`, `customer_id`) **chưa được expose** qua command line (có thể thêm sau nếu cần)

**Các filter khác có thể thêm (chưa implement):**

`BillExtractor.fetch_bills()` hỗ trợ các tham số sau nhưng chưa được expose qua command line:

| Tham số | Type | Mô tả | Ví dụ |
|---------|------|-------|-------|
| `modes` | List[int] | Kiểu xuất nhập kho | `[2]` = Bán lẻ, `[6]` = Bán buôn |
| `bill_type` | int | Loại xuất nhập kho | `1` = Nhập kho, `2` = Xuất kho |
| `customer_id` | int | Lọc theo ID khách hàng | `12345` |
| `process_by_day` | bool | Xử lý từng ngày riêng biệt | `True`/`False` |

**Các giá trị `modes` phổ biến:**
- `2` = Bán lẻ
- `6` = Bán buôn
- `5` = Nhà cung cấp
- `3` = Chuyển kho
- `1` = Giao hàng
- `8` = Bù trừ kiểm kho
- `13` = Bảo hành
- `15` = Sửa chữa
- `16` = Linh kiện bảo hành
- `19` = Combo
- `10` = Khác

**Xử lý tham số:**

```python
# Parse date range nếu có
if args.from_date and args.to_date:
    from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d")
    to_date = to_date.replace(hour=23, minute=59, second=59)
    incremental = False  # Dùng date range → không incremental
elif args.from_date or args.to_date:
    # Phải có cả 2 hoặc không có gì
    logger.error("Both --from-date and --to-date must be provided.")
    sys.exit(1)
else:
    # Không có date range → dùng incremental
    from_date = None
    to_date = None
    incremental = True
```

**Khởi tạo components:**

```python
loader = GCSLoader(bucket_name=settings.bronze_bucket)
watermark_tracker = WatermarkTracker()
```

**Gọi extraction:**

```python
extract_entity(
    platform="nhanh",           # Hard-coded cho feature này
    entity="bills",             # Hard-coded cho feature này
    loader=loader,
    watermark_tracker=watermark_tracker,
    incremental=incremental,    # True nếu không có date range
    from_date=from_date,        # None nếu incremental
    to_date=to_date              # None nếu incremental
)
```

**Setup BigQuery External Tables:**

```python
bq_setup = BigQueryExternalTableSetup()
bq_setup.setup_all_tables(platforms=["nhanh"])  # Setup cho tất cả entities của nhanh
```

---

## 2. Xử Lý Extraction Logic

### File: `src/pipeline/extraction.py`

**Chức năng:** Logic chung để extract và load entities từ các platforms vào Bronze layer.

### Hàm `extract_entity()`

**Tham số đầu vào:**

```python
extract_entity(
    platform: str,              # 'nhanh'
    entity: str,                # 'bills'
    loader: GCSLoader,          # Instance để upload lên GCS
    watermark_tracker: WatermarkTracker,  # Track incremental extraction
    incremental: bool = True,   # Có dùng incremental không
    from_date: Optional[datetime] = None,  # Ngày bắt đầu
    to_date: Optional[datetime] = None,  # Ngày kết thúc
    **extractor_kwargs          # Tham số riêng cho extractor
)
```

**Luồng xử lý:**

1. **Lấy extractor từ registry:**
   ```python
   platform_extractors = FEATURE_REGISTRY.get(platform)  # {"bills": BillExtractor}
   extractor_class = platform_extractors.get(entity)      # BillExtractor
   extractor = extractor_class()                          # Tạo instance
   ```

2. **Xác định date range:**

   **Nếu incremental và không có from_date/to_date:**
   ```python
   watermark_key = f"{platform}_{entity}"  # "nhanh_bills"
   incr_from, incr_to = watermark_tracker.get_incremental_range(
       watermark_key,
       lookback_hours=1  # Nếu không có watermark, lấy 1 giờ gần nhất
   )
   extractor_kwargs["updated_at_from"] = incr_from
   extractor_kwargs["updated_at_to"] = incr_to
   ```

   **Nếu có from_date và to_date:**
   ```python
   extractor_kwargs["from_date"] = from_date
   extractor_kwargs["to_date"] = to_date
   ```

3. **Xử lý đặc biệt cho Nhanh bills:**

   ```python
   if platform == "nhanh" and entity == "bills":
       # Tách bills và products riêng biệt
       bills_data, products_data = extractor.extract_with_products(**extractor_kwargs)
       
       # Upload riêng bills và bill_products
       _upload_and_log(loader, platform, entity, bills_data, partition_date, incremental)
       _upload_and_log(loader, platform, "bill_products", products_data, partition_date, incremental)
   ```

---

## 3. Watermark Tracker (Incremental Extraction)

### File: `src/loaders/watermark.py`

**Chức năng:** Quản lý watermark để track lần extraction cuối cùng, hỗ trợ incremental extraction.

**Cơ chế hoạt động:**

1. **Lưu watermark trong BigQuery:**
   - Table: `bronze.extraction_watermarks`
   - Schema:
     - `entity`: Tên entity (ví dụ: "nhanh_bills")
     - `last_extracted_at`: Timestamp lần extraction cuối
     - `last_successful_run`: Timestamp lần chạy thành công cuối
     - `records_count`: Số lượng records đã extract
     - `updated_at`: Thời gian cập nhật

2. **Lấy incremental range:**

   ```python
   def get_incremental_range(entity: str, lookback_hours: int = 1):
       watermark = self.get_watermark(entity)  # Lấy từ BigQuery
       to_date = datetime.now(timezone.utc)
       
       if watermark:
           from_date = watermark  # Bắt đầu từ watermark cuối
       else:
           # Không có watermark: dùng lookback period
           from_date = to_date - timedelta(hours=lookback_hours)
       
       return from_date, to_date
   ```

3. **Cập nhật watermark sau extraction:**

   ```python
   watermark_tracker.update_watermark(
       f"{platform}_{entity}",
       datetime.utcnow(),
       len(data)
   )
   ```

---

## 4. Nhanh API Client

### File: `src/shared/nhanh/client.py`

**Chức năng:** Client để gọi Nhanh API với các tính năng:
- Authentication tự động
- Rate limiting (150 requests/30s)
- Pagination handling
- Error handling và retry
- Chia date range thành chunks 31 ngày

### Authentication

**Credentials được lấy từ Secret Manager:**

```python
credentials = {
    "appId": "từ secret: nhanh-app-id",
    "businessId": "từ secret: nhanh-business-id",
    "accessToken": "từ secret: nhanh-access-token"
}
```

**Request headers:**
```python
headers = {
    "Content-Type": "application/json",
    "Authorization": credentials["accessToken"]
}
```

**Request params:**
```python
params = {
    "appId": credentials["appId"],
    "businessId": credentials["businessId"]
}
```

### Rate Limiting

**Token Bucket Algorithm:**

```python
token_bucket = TokenBucket(
    capacity=150,           # 150 requests
    refill_rate=5.0         # 5 tokens/giây (150/30s)
)
```

**Cơ chế:**
- Mỗi request cần 1 token
- Nếu không đủ token → đợi cho đến khi có đủ
- Tự động refill tokens theo thời gian

### Pagination

**Hàm `fetch_paginated()`:**

```python
def fetch_paginated(endpoint: str, body: Dict, data_key: str = "data"):
    all_data = []
    page_num = 1
    
    while True:
        response = self._make_request(endpoint, body)
        page_data = response.get(data_key, [])
        all_data.extend(page_data)
        
        next_page = response.get("paginator", {}).get("next")
        if not next_page:
            break
        
        body["paginator"]["next"] = next_page
        page_num += 1
    
    return all_data
```

**Request body mẫu:**
```json
{
  "filters": {
    "fromDate": "2024-01-01",
    "toDate": "2024-01-31",
    "modes": [2],
    "type": 2
  },
  "paginator": {
    "size": 50
  },
  "dataOptions": {
    "tags": 1,
    "giftProducts": 1
  }
}
```

### Chia Date Range

**Giới hạn API:** Tối đa 31 ngày mỗi request

**Hàm `split_date_range()`:**

```python
def split_date_range(from_date: datetime, to_date: datetime):
    max_days = 31
    chunks = []
    current_start = from_date
    
    while current_start < to_date:
        current_end = min(
            current_start + timedelta(days=max_days - 1),
            to_date
        )
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    
    return chunks
```

**Ví dụ:** Date range 60 ngày → 2 chunks (31 ngày + 29 ngày)

---

## 5. Bill Extractor

### File: `src/features/nhanh/bills/extractor.py`

**Chức năng:** Extract bills từ Nhanh API với xử lý đặc biệt cho products.

### Hàm `fetch_bills()`

**Tham số:**

```python
fetch_bills(
    from_date: Optional[datetime] = None,      # Filter theo bill date
    to_date: Optional[datetime] = None,
    modes: Optional[List[int]] = None,         # [2] = bán lẻ
    bill_type: Optional[int] = None,           # 1 = nhập kho, 2 = xuất kho
    customer_id: Optional[int] = None,         # Lọc theo khách hàng
    updated_at_from: Optional[datetime] = None, # Filter theo updatedAt (incremental)
    updated_at_to: Optional[datetime] = None,
    process_by_day: bool = False               # Xử lý từng ngày riêng
)
```

**Luồng xử lý:**

1. **Xác định date range và field:**

   ```python
   if updated_at_from and updated_at_to:
       date_chunks = client.split_date_range(updated_at_from, updated_at_to)
       date_field = "updatedAtFrom"
       date_to_field = "updatedAtTo"
   elif from_date and to_date:
       date_chunks = client.split_date_range(from_date, to_date)
       date_field = "fromDate"
       date_to_field = "toDate"
   ```

2. **Gọi API cho mỗi chunk:**

   ```python
   for chunk_from, chunk_to in date_chunks:
       filters = {
           date_field: chunk_from.strftime("%Y-%m-%d"),
           date_to_field: chunk_to.strftime("%Y-%m-%d")
       }
       if modes:
           filters["modes"] = modes
       
       body = {
           "filters": filters,
           "paginator": {"size": 50},
           "dataOptions": {"tags": 1, "giftProducts": 1}
       }
       
       chunk_bills = client.fetch_paginated("/bill/list", body)
       all_bills.extend(chunk_bills)
   ```

### Hàm `extract_with_products()`

**Chức năng:** Tách bills và products thành 2 danh sách riêng biệt.

**Luồng xử lý:**

```python
def extract_with_products(...):
    all_bills = self.fetch_bills(...)
    
    bills_without_products = []
    all_products = []
    
    for bill in all_bills:
        bill_copy = bill.copy()
        products_data = bill_copy.pop('products', [])  # Tách products ra
        
        bill_copy['bill_id'] = bill.get('id')
        bills_without_products.append(bill_copy)
        
        # Xử lý products
        if isinstance(products_data, dict):
            for product_id, product_data in products_data.items():
                product_record = product_data.copy()
                product_record['bill_id'] = bill_copy['bill_id']
                product_record['product_id'] = product_id
                all_products.append(product_record)
        elif isinstance(products_data, list):
            for product_data in products_data:
                product_record = product_data.copy()
                product_record['bill_id'] = bill_copy['bill_id']
                all_products.append(product_record)
    
    return bills_without_products, all_products
```

**Kết quả:**
- `bills_without_products`: Danh sách bills không có field `products`
- `all_products`: Danh sách products với `bill_id` và `product_id`

---

## 6. GCS Loader

### File: `src/shared/gcs/loader.py`

**Chức năng:** Upload data lên Google Cloud Storage dưới dạng Parquet với partitioning.

### Partitioning Strategy

**2 loại partitioning:**

1. **Month level:** `year={YYYY}/month={MM}/`
2. **Day level:** `year={YYYY}/month={MM}/day={DD}/` (performance tốt hơn)

**Cấu hình:** `settings.partition_strategy` (mặc định: "month")

### Hàm `upload_parquet_by_date()`

**Tham số:**

```python
upload_parquet_by_date(
    entity: str,                    # "nhanh/bills"
    data: List[Dict[str, Any]],     # Danh sách records
    partition_date: Optional[date] = None,  # Ngày partition
    date_field: str = "date",       # Field chứa date trong data
    metadata: Optional[Dict] = None,
    overwrite_partition: bool = True  # Xóa file cũ trong partition
)
```

**Luồng xử lý:**

1. **Xác định partition date:**

   ```python
   if partition_date is None:
       # Parse từ date_field trong data
       date_str = data[0][date_field]
       partition_date = datetime.fromisoformat(date_str).date()
   ```

2. **Tạo partition path:**

   ```python
   partition_path = f"{entity}/year={year}/month={month:02d}/day={day:02d}/"
   # Ví dụ: "nhanh/bills/year=2024/month=01/day=15/"
   ```

3. **Xóa file cũ nếu overwrite:**

   ```python
   if overwrite_partition:
       # Xóa tất cả file .parquet trong partition
       self._delete_partition_files(partition_path, ".parquet", partition_date)
   ```

4. **Convert sang Parquet và upload:**

   ```python
   df = pd.DataFrame(data)
   table = pa.Table.from_pandas(df)
   parquet_buffer = BytesIO()
   pq.write_table(table, parquet_buffer, compression='snappy')
   
   filename = f"data_{partition_date.isoformat()}_{timestamp}.parquet"
   full_path = f"{partition_path}{filename}"
   
   blob = bucket.blob(full_path)
   blob.upload_from_string(parquet_bytes, content_type='application/parquet')
   ```

**GCS Path mẫu:**
```
gs://bronze-bucket/nhanh/bills/year=2024/month=01/day=15/data_2024-01-15_20240115_120000_123456.parquet
```

---

## 7. BigQuery External Table Setup

### File: `src/loaders/bigquery_setup.py`

**Chức năng:** Tạo/cập nhật BigQuery External Tables trỏ đến GCS files.

### Hàm `setup_external_table()`

**Tham số:**

```python
setup_external_table(
    platform: str,      # "nhanh"
    entity: str,        # "bills" hoặc "bill_products"
    table_name: Optional[str] = None  # Mặc định: "{platform}_{entity}_raw"
)
```

**Luồng xử lý:**

1. **Tạo table name:**

   ```python
   table_name = f"{platform}_{entity}_raw"
   # Ví dụ: "nhanh_bills_raw", "nhanh_bill_products_raw"
   ```

2. **Tạo GCS URI pattern:**

   ```python
   gcs_uri = f"gs://{bronze_bucket}/{platform}/{entity}/*.parquet"
   # Ví dụ: "gs://bronze-bucket/nhanh/bills/*.parquet"
   ```

3. **Tạo External Table bằng SQL:**

   ```sql
   CREATE OR REPLACE EXTERNAL TABLE `project.bronze.nhanh_bills_raw`
   OPTIONS (
     format = 'PARQUET',
     uris = ['gs://bronze-bucket/nhanh/bills/*.parquet']
   )
   ```

**Kết quả:**

- External Table được tạo trong dataset `bronze`
- Table trỏ đến tất cả file `.parquet` trong GCS path
- Có thể query trực tiếp từ BigQuery mà không cần load data vào BigQuery

**Ví dụ query:**

```sql
SELECT *
FROM `project.bronze.nhanh_bills_raw`
WHERE DATE(created_at) = '2024-01-15'
LIMIT 100
```

---

## 8. Tổng Kết Luồng Hoàn Chỉnh

### Ví dụ: Extract bills từ 2024-01-01 đến 2024-01-31

1. **Command:**
   ```bash
   python src/features/nhanh/bills/extract.py --from-date 2024-01-01 --to-date 2024-01-31
   ```

2. **extract.py:**
   - Parse arguments → `from_date=2024-01-01`, `to_date=2024-01-31`
   - Xác định `incremental=False` (vì có date range)
   - Khởi tạo `GCSLoader`, `WatermarkTracker`
   - Gọi `extract_entity("nhanh", "bills", incremental=False, from_date=..., to_date=...)`

3. **extraction.py:**
   - Lấy `BillExtractor` từ registry
   - Vì có `from_date`/`to_date` → không dùng incremental
   - Gọi `extractor.extract_with_products(from_date=..., to_date=...)`

4. **extractor.py:**
   - Chia date range thành chunks (31 ngày mỗi chunk)
   - Với mỗi chunk:
     - Gọi `client.fetch_paginated("/bill/list", body)`
     - Xử lý pagination → lấy tất cả bills
   - Tách bills và products
   - Return `(bills_data, products_data)`

5. **NhanhApiClient:**
   - Với mỗi request:
     - Check rate limit → đợi nếu cần
     - Gọi API với authentication
     - Xử lý pagination → lấy tất cả pages
   - Return danh sách bills

6. **extraction.py (tiếp):**
   - Upload bills: `loader.upload_parquet_by_date("nhanh/bills", bills_data, partition_date)`
   - Upload products: `loader.upload_parquet_by_date("nhanh/bill_products", products_data, partition_date)`

7. **GCSLoader:**
   - Với mỗi partition date:
     - Tạo partition path: `nhanh/bills/year=2024/month=01/day=01/`
     - Xóa file cũ trong partition (nếu overwrite)
     - Convert data → Parquet
     - Upload lên GCS

8. **extract.py (tiếp):**
   - Setup BigQuery External Tables:
     ```python
     bq_setup.setup_all_tables(platforms=["nhanh"])
     ```

9. **BigQueryExternalTableSetup:**
   - Tạo External Table `bronze.nhanh_bills_raw` → trỏ đến `gs://bronze-bucket/nhanh/bills/*.parquet`
   - Tạo External Table `bronze.nhanh_bill_products_raw` → trỏ đến `gs://bronze-bucket/nhanh/bill_products/*.parquet`

10. **Kết quả:**
    - Data đã được upload lên GCS dưới dạng Parquet
    - BigQuery External Tables đã được tạo/cập nhật
    - Có thể query data từ BigQuery

---

## 9. Các Tham Số Quan Trọng

### Environment Variables

| Biến | Mô tả | Mặc định |
|------|-------|----------|
| `GCP_PROJECT` | GCP Project ID | `sync-nhanhvn-project` |
| `BRONZE_BUCKET` | GCS bucket cho Bronze layer | `sync-nhanhvn-project` |
| `BRONZE_DATASET` | BigQuery dataset cho Bronze | `bronze` |
| `NHANH_API_BASE_URL` | Base URL của Nhanh API | `https://pos.open.nhanh.vn` |
| `NHANH_RATE_LIMIT` | Số requests tối đa | `150` |
| `NHANH_RATE_WINDOW` | Time window cho rate limit (giây) | `30` |
| `NHANH_MAX_DATE_RANGE_DAYS` | Số ngày tối đa mỗi request | `31` |
| `PARTITION_STRATEGY` | Chiến lược partition (day/month) | `month` |

### Secrets (Secret Manager)

| Secret Name | Mô tả |
|-------------|-------|
| `nhanh-app-id` | App ID của ứng dụng |
| `nhanh-business-id` | Business ID trên Nhanh.vn |
| `nhanh-access-token` | Access token để authenticate |

---

## 10. Lưu Ý Quan Trọng

### Incremental vs Full Sync

- **Incremental:** Chỉ lấy data từ lần extraction cuối (dựa trên watermark)
- **Full Sync:** Lấy tất cả data trong date range
- **Date Range:** Khi có `--from-date` và `--to-date`, tự động disable incremental

### Date Range Splitting

- API Nhanh giới hạn tối đa 31 ngày mỗi request
- Hệ thống tự động chia date range lớn hơn thành các chunks 31 ngày
- Có thể xử lý từng ngày riêng biệt với `process_by_day=True`

### Products Separation

- Bills và products được tách riêng thành 2 tables:
  - `nhanh_bills_raw`: Chứa bills (không có field `products`)
  - `nhanh_bill_products_raw`: Chứa products với `bill_id` và `product_id`

### Partition Overwrite

- Mặc định: `overwrite_partition=True`
- Khi upload data mới cho cùng partition date → xóa file cũ trước
- Đảm bảo mỗi partition chỉ có 1 file mới nhất

### External Tables

- External Tables không lưu data trong BigQuery
- Chỉ trỏ đến GCS files
- Query sẽ đọc trực tiếp từ GCS
- Tiết kiệm storage cost nhưng có thể chậm hơn native tables

---

## 11. Ví Dụ Sử Dụng

### Incremental Extraction (Mặc định)

```bash
python src/features/nhanh/bills/extract.py
```

- **Không cần tham số** → Tự động dùng incremental extraction
- Lấy data từ lần extraction cuối (watermark)
- Nếu không có watermark → lấy 1 giờ gần nhất

### Full Sync với Date Range

```bash
python src/features/nhanh/bills/extract.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31
```

- Lấy tất cả bills từ 2024-01-01 đến 2024-01-31
- Tự động chia thành chunks 31 ngày
- Tự động disable incremental khi có date range

### Chạy trên Cloud Run Jobs

```bash
# Execute với date range
gcloud run jobs execute nhanh-etl-job \
  --region=asia-southeast1 \
  --args="python,src/features/nhanh/bills/extract.py,--from-date,2024-01-01,--to-date,2024-01-31"

# Execute incremental (không có args)
gcloud run jobs execute nhanh-etl-job \
  --region=asia-southeast1 \
  --args="python,src/features/nhanh/bills/extract.py"
```


---

## 12. Monitoring và Debugging

### Logs

- Tất cả logs được ghi với structured logging
- Có thể filter theo `platform`, `entity`, `chunk`, `page`, etc.

### Watermark Tracking

- Kiểm tra watermark trong BigQuery:
  ```sql
  SELECT *
  FROM `project.bronze.extraction_watermarks`
  WHERE entity = 'nhanh_bills'
  ORDER BY last_extracted_at DESC
  ```

### GCS Files

- Kiểm tra files đã upload:
  ```bash
  gsutil ls gs://bronze-bucket/nhanh/bills/year=2024/month=01/day=15/
  ```

### BigQuery External Tables

- Kiểm tra External Tables:
  ```sql
  SELECT *
  FROM `project.bronze.INFORMATION_SCHEMA.TABLES`
  WHERE table_type = 'EXTERNAL'
  ```

---

*Tài liệu được tạo tự động từ codebase - Cập nhật: 2024-01-15*

