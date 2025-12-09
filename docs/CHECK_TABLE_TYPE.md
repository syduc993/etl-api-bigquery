# Cách Kiểm Tra Loại Bảng trong BigQuery

Tài liệu này mô tả các cách để phân biệt **External Tables** và **Native Tables** trong BigQuery.

---

## Tổng Quan

| Loại | Mô tả | Storage | Performance |
|------|-------|---------|-------------|
| **External Table** | Trỏ đến file trong GCS | GCS | Chậm hơn (đọc từ GCS) |
| **Native Table** | Data lưu trong BigQuery | BigQuery Storage | Nhanh hơn (có partition/cluster) |

---

## Method 1: Query INFORMATION_SCHEMA

Cách đơn giản nhất là query `INFORMATION_SCHEMA.TABLES`:

```sql
SELECT 
  table_name,
  table_type,
  CASE 
    WHEN table_type = 'EXTERNAL' THEN 'External Table'
    WHEN table_type = 'BASE TABLE' THEN 'Native Table'
    WHEN table_type = 'VIEW' THEN 'View'
    ELSE table_type
  END as table_type_description
FROM `sync-nhanhvn-project.bronze.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE 'nhanh%'
ORDER BY table_name;
```

**Kết quả:**
```
table_name              | table_type | table_type_description
------------------------|------------|------------------------
nhanh_bills_raw         | EXTERNAL   | External Table
nhanh_bill_products_raw | EXTERNAL   | External Table
```

---

## Method 2: Kiểm Tra Metadata của Table

### Sử dụng BigQuery Python Client

```python
from google.cloud import bigquery

client = bigquery.Client(project="sync-nhanhvn-project")

# Check External Table
table_ref = client.dataset("bronze").table("nhanh_bills_raw")
table = client.get_table(table_ref)

print(f"Table Type: {table.table_type}")  # EXTERNAL
print(f"Has External Config: {table.external_data_configuration is not None}")  # True
if table.external_data_configuration:
    print(f"Source Format: {table.external_data_configuration.source_format}")  # PARQUET
    print(f"Source URIs: {table.external_data_configuration.source_uris}")

# Check Native Table
table_ref = client.dataset("nhanhVN").table("fact_sales_bills_v3_0")
table = client.get_table(table_ref)

print(f"Table Type: {table.table_type}")  # TABLE
print(f"Has External Config: {table.external_data_configuration is not None}")  # False
print(f"Num Bytes: {table.num_bytes}")  # Có giá trị
print(f"Num Rows: {table.num_rows}")  # Có giá trị
```

### Sử dụng BigQuery API (REST)

```bash
# Check External Table
curl -X GET \
  "https://bigquery.googleapis.com/bigquery/v2/projects/sync-nhanhvn-project/datasets/bronze/tables/nhanh_bills_raw" \
  -H "Authorization: Bearer $ACCESS_TOKEN"

# Response sẽ có:
# {
#   "type": "EXTERNAL",
#   "externalDataConfiguration": {
#     "sourceFormat": "PARQUET",
#     "sourceUris": ["gs://bucket/path/*.parquet"]
#   }
# }
```

---

## Method 3: Kiểm Tra trong BigQuery Console

1. Mở BigQuery Console: https://console.cloud.google.com/bigquery
2. Tìm table trong dataset
3. Click vào table name
4. Xem tab **Details**:
   - **External Table**: Sẽ hiển thị "External data source" với GCS URI
   - **Native Table**: Sẽ hiển thị "Table size", "Number of rows", "Partitioning", "Clustering"

---

## Method 4: SQL Query để Kiểm Tra Tất Cả Tables

```sql
SELECT 
  table_catalog as project,
  table_schema as dataset,
  table_name,
  table_type,
  CASE 
    WHEN table_type = 'EXTERNAL' THEN '✅ External Table (đọc từ GCS)'
    WHEN table_type = 'BASE TABLE' THEN '✅ Native Table (lưu trong BigQuery)'
    WHEN table_type = 'VIEW' THEN '📊 View'
    ELSE table_type
  END as description
FROM `sync-nhanhvn-project.bronze.INFORMATION_SCHEMA.TABLES`
UNION ALL
SELECT 
  table_catalog,
  table_schema,
  table_name,
  table_type,
  CASE 
    WHEN table_type = 'EXTERNAL' THEN '✅ External Table (đọc từ GCS)'
    WHEN table_type = 'BASE TABLE' THEN '✅ Native Table (lưu trong BigQuery)'
    WHEN table_type = 'VIEW' THEN '📊 View'
    ELSE table_type
  END
FROM `sync-nhanhvn-project.nhanhVN.INFORMATION_SCHEMA.TABLES`
ORDER BY table_schema, table_name;
```

---

## Method 5: Script Python để Kiểm Tra

Tạo file `check_table_type.py`:

```python
"""Script để kiểm tra loại table trong BigQuery."""
from google.cloud import bigquery
from src.config import settings

def check_table_type(project_id: str, dataset_id: str, table_id: str):
    """Kiểm tra loại table."""
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    print(f"\nTable: {project_id}.{dataset_id}.{table_id}")
    print(f"  Type: {table.table_type}")
    
    if table.table_type == "EXTERNAL":
        print("  ✅ External Table (đọc từ GCS)")
        if table.external_data_configuration:
            print(f"  Source Format: {table.external_data_configuration.source_format}")
            print(f"  Source URIs: {table.external_data_configuration.source_uris[:1]}...")
    elif table.table_type == "TABLE":
        print("  ✅ Native Table (lưu trong BigQuery)")
        print(f"  Size: {table.num_bytes:,} bytes")
        print(f"  Rows: {table.num_rows:,}")
        if table.time_partitioning:
            print(f"  Partitioned by: {table.time_partitioning.field}")
        if table.clustering_fields:
            print(f"  Clustered by: {table.clustering_fields}")
    elif table.table_type == "VIEW":
        print("  📊 View")
    
    return table.table_type

# Example usage
if __name__ == "__main__":
    project = settings.gcp_project
    
    # Check External Table
    check_table_type(project, "bronze", "nhanh_bills_raw")
    
    # Check Native Table
    check_table_type(project, "nhanhVN", "fact_sales_bills_v3_0")
```

---

## Dấu Hiệu Nhận Biết

### External Table:
- ✅ `table_type = 'EXTERNAL'` trong INFORMATION_SCHEMA
- ✅ Có `external_data_configuration` trong metadata
- ✅ Không có `num_bytes` hoặc `num_rows` (hoặc = 0)
- ✅ Trong Console: Hiển thị "External data source" với GCS URI
- ✅ Không thể có partition/cluster (BigQuery tự động partition khi query)

### Native Table:
- ✅ `table_type = 'BASE TABLE'` hoặc `'TABLE'` trong INFORMATION_SCHEMA
- ✅ Không có `external_data_configuration`
- ✅ Có `num_bytes` và `num_rows` > 0
- ✅ Trong Console: Hiển thị "Table size", "Number of rows"
- ✅ Có thể có partition/cluster (được định nghĩa trong schema)

---

## Ví Dụ Thực Tế

### External Tables trong Project:
- `bronze.nhanh_bills_raw` → External Table (Parquet từ GCS)
- `bronze.nhanh_bill_products_raw` → External Table (Parquet từ GCS)

### Native Tables trong Project:
- `nhanhVN.fact_sales_bills_v3_0` → Native Table (partitioned by date)
- `nhanhVN.fact_sales_bills_product_v3_0` → Native Table (partitioned by extraction_timestamp)

---

## Lưu Ý

1. **External Tables** không tốn storage cost trong BigQuery (chỉ tốn GCS)
2. **Native Tables** tốn storage cost trong BigQuery nhưng query nhanh hơn
3. External Tables có thể được convert sang Native Tables bằng `CREATE TABLE AS SELECT`
4. Native Tables có thể được export sang GCS và tạo External Table từ đó

---

*Tài liệu được tạo: 2025-12-09*

