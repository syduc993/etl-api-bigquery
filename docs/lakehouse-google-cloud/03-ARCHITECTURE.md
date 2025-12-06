# Kiến trúc và Patterns cho Lakehouse trên Google Cloud

## Kiến trúc tổng quan

### Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Presentation & Analytics Layer              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────┐  │
│  │   BI    │  │   ML    │  │   API   │  │ Analytics│  │
│  └─────────┘  └─────────┘  └─────────┘  └──────────┘  │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Processing & Query Layer                    │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐        │
│  │  BigQuery  │  │  Dataflow  │  │  Dataproc  │        │
│  └────────────┘  └────────────┘  └────────────┘        │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Storage Layer (Medallion Architecture)      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Bronze     │  │   Silver    │  │    Gold     │    │
│  │  (Raw)      │  │  (Cleaned)  │  │  (Curated)  │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Data Sources                                │
│  (APIs, Databases, Files, Streams)                      │
└─────────────────────────────────────────────────────────┘
```

## Medallion Architecture

### Bronze Layer (Raw Data)

**Mục đích**: Lưu trữ dữ liệu gốc, chưa qua xử lý

**Đặc điểm**:
- Raw data từ source systems
- As-is format
- Append-only
- Immutable

**Implementation**:
- Cloud Storage buckets
- Partitioned by ingestion date
- Minimal transformation

**Ví dụ Structure**:
```
gs://project-data-lake/bronze/
├── orders/
│   ├── year=2024/
│   │   ├── month=12/
│   │   │   ├── day=06/
│   │   │   │   └── orders_20241206_001.parquet
```

### Silver Layer (Cleaned Data)

**Mục đích**: Dữ liệu đã được làm sạch và validate

**Đặc điểm**:
- Data quality checks
- Schema validation
- Deduplication
- Basic transformations

**Implementation**:
- BigLake tables với Iceberg/Delta format
- Quality rules với Dataplex
- Automated processing với Dataflow/Dataproc

**Ví dụ Structure**:
```
gs://project-data-lake/silver/
├── orders/
│   └── orders_cleaned.iceberg/
├── customers/
│   └── customers_cleaned.iceberg/
```

### Gold Layer (Curated Data)

**Mục đích**: Dữ liệu business-ready cho analytics

**Đặc điểm**:
- Aggregated data
- Business logic applied
- Optimized for queries
- Ready for consumption

**Implementation**:
- BigQuery tables
- Materialized views
- Optimized partitions và clustering

**Ví dụ Structure**:
```
project.dataset.gold/
├── daily_sales_summary
├── customer_lifetime_value
├── product_revenue_metrics
```

## Data Flow Patterns

### Pattern 1: Batch Ingestion

```
Source → Cloud Storage (Bronze) → Dataflow/Dataproc → BigLake (Silver) → BigQuery (Gold)
```

**Use Case**: 
- Scheduled data loads
- Historical data migration
- Large batch processing

**Implementation**:
- Cloud Run Jobs cho orchestration Python scripts
- Cloud Scheduler cho scheduling
- Dataflow cho transformation
- Incremental loads với watermark

### Pattern 2: Streaming Ingestion

```
Source → Pub/Sub → Dataflow → Cloud Storage (Bronze) → BigLake (Silver) → BigQuery (Gold)
```

**Use Case**:
- Real-time data ingestion
- Event-driven processing
- Low-latency requirements

**Implementation**:
- Pub/Sub cho message queuing
- Dataflow streaming pipelines
- Windowing và state management

### Pattern 3: Change Data Capture (CDC)

```
Database → Datastream → Cloud Storage → Dataflow → BigLake → BigQuery
```

**Use Case**:
- Replicate database changes
- Real-time synchronization
- Data warehouse updates

**Implementation**:
- Datastream cho CDC
- Debezium format
- Upsert logic trong Dataflow

## Storage Patterns

### Pattern 1: Multi-Format Support

```
Raw Data (Parquet)
    ↓
Silver (Iceberg)  ← Query với BigQuery
    ↓
Gold (BigQuery Native)  ← Analytics optimized
```

### Pattern 2: Partitioning Strategy

**Time-based Partitioning**:
- Partition by date/hour
- Optimize query performance
- Easy data management

**Example**:
```sql
CREATE TABLE orders (
  order_id STRING,
  order_date DATE,
  amount FLOAT64
)
PARTITION BY order_date
CLUSTER BY customer_id
```

**Hash Partitioning**:
- For non-temporal data
- Even distribution
- Good for joins

### Pattern 3: Data Lifecycle Management

```
Hot Data (Standard Storage) → Warm Data (Nearline) → Cold Data (Archive)
```

**Implementation**:
- Cloud Storage lifecycle policies
- Automatic transitions
- Cost optimization

## Query Patterns

### Pattern 1: Direct Query

```
Application → BigQuery → BigLake Tables → Cloud Storage
```

**Use Case**: 
- Ad-hoc queries
- Reporting
- Analytics

### Pattern 2: Materialized Views

```
Raw Data → Materialized View → Fast Query Results
```

**Benefits**:
- Pre-aggregated data
- Faster query performance
- Automatic refresh

### Pattern 3: Federated Queries

```
BigQuery → BigLake → AWS S3 / Azure ADLS
```

**Use Case**:
- Multi-cloud scenarios
- Cross-platform analytics
- Avoid data movement

## Security Patterns

### Pattern 1: Fine-Grained Access Control

- **Column-level security**: Mask sensitive columns
- **Row-level security**: Filter rows based on user
- **Dynamic data masking**: Real-time masking

**Implementation**:
- BigLake với IAM policies
- Authorized views
- Policy tags

### Pattern 2: Data Encryption

- **At rest**: Encryption với Cloud KMS
- **In transit**: TLS/SSL
- **Customer-managed keys**: Full control

### Pattern 3: Access Audit

- **Cloud Audit Logs**: Track all access
- **Data Catalog**: Track data usage
- **Dataplex**: Monitor data access patterns

## Governance Patterns

### Pattern 1: Data Catalog

```
Dataplex → Data Discovery → Metadata Management → Lineage Tracking
```

**Components**:
- Automatic schema discovery
- Data profiling
- Business glossary
- Data lineage

### Pattern 2: Data Quality

```
Data → Quality Checks → Quality Score → Alert/Block if failed
```

**Implementation**:
- Dataplex Data Quality
- Custom quality rules
- Automated monitoring

### Pattern 3: Policy Enforcement

```
Policy Definition → Policy Engine → Enforcement → Monitoring
```

**Types**:
- Access policies
- Data retention policies
- Data classification policies

## Cost Optimization Patterns

### Pattern 1: Storage Tiering

- Hot data: Standard storage
- Warm data: Nearline
- Cold data: Archive

### Pattern 2: Compute Optimization

- Use preemptible VMs trong Dataproc
- Right-size BigQuery slots
- Use query result caching

### Pattern 3: Partition Pruning

- Partition tables để giảm data scanned
- Use WHERE clauses với partition columns
- Cluster tables cho better pruning

## High-Level Reference Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    External Systems                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  APIs    │  │ Databases│  │  Files   │  │  Streams │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼─────────────┼──────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                      │
        ┌─────────────▼─────────────┐
        │    Ingestion Layer        │
        │  ┌────────┐  ┌────────┐  │
        │  │Pub/Sub │  │Dataflow│  │
        │  └────────┘  └────────┘  │
        └─────────────┬─────────────┘
                      │
        ┌─────────────▼───────────────────────────────────┐
        │         Storage (Medallion)                     │
        │  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
        │  │  Bronze  │  │  Silver  │  │   Gold   │     │
        │  │  (GCS)   │  │ (BigLake)│  │(BigQuery)│     │
        │  └──────────┘  └──────────┘  └──────────┘     │
        └─────────────┬───────────────────────────────────┘
                      │
        ┌─────────────▼─────────────┐
        │    Governance (Dataplex)   │
        │  Catalog | Quality | Lineage│
        └─────────────┬─────────────┘
                      │
        ┌─────────────▼─────────────┐
        │    Consumption Layer      │
        │  ┌────────┐  ┌────────┐  │
        │  │   BI   │  │   ML   │  │
        │  └────────┘  └────────┘  │
        └───────────────────────────┘
```

## Best Practices Summary

1. **Separation of Concerns**: Tách biệt storage, processing, và analytics
2. **Medallion Architecture**: Bronze → Silver → Gold
3. **Open Formats**: Sử dụng Iceberg/Delta/Hudi cho flexibility
4. **Incremental Processing**: Process only changed data
5. **Partitioning**: Partition by time hoặc business keys
6. **Monitoring**: Monitor data quality, cost, và performance
7. **Security**: Implement least privilege access
8. **Documentation**: Maintain data catalog và documentation

## Tài liệu tham khảo

- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Data Lakehouse Architecture Patterns](https://cloud.google.com/solutions/data-lakehouse)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

