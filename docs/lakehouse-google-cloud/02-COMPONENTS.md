# Các thành phần chính của Lakehouse trên Google Cloud

## 1. BigQuery

### Giới thiệu

BigQuery là một serverless, highly-scalable data warehouse được Google Cloud cung cấp, và hiện tại đã phát triển thành một unified analytics platform hỗ trợ lakehouse architecture.

### Tính năng chính

#### 1.1. BigQuery Native Tables
- **Managed Storage**: Dữ liệu được lưu trữ và quản lý bởi BigQuery
- **Optimized Format**: Sử dụng Columnar format (Capacitor) được tối ưu hóa
- **High Performance**: Query performance cao nhờ automatic partitioning và clustering
- **Full SQL Support**: Hỗ trợ Standard SQL với các functions mở rộng

#### 1.2. BigQuery External Tables
- Query dữ liệu trực tiếp từ Cloud Storage
- Hỗ trợ các format: CSV, JSON, Avro, Parquet, ORC
- No data duplication
- Cost-effective cho dữ liệu không thường xuyên truy vấn

#### 1.3. BigLake Tables
- Unified interface cho open formats (Iceberg, Delta, Hudi)
- Fine-grained access control
- Multi-cloud support (query data từ AWS S3, Azure ADLS)
- Separation of compute và storage

### Use Cases

- **Data Warehousing**: Traditional analytics và reporting
- **Lakehouse**: Query open format tables
- **Real-time Analytics**: Stream processing với BigQuery streaming
- **ML Integration**: Built-in ML functions và AutoML

## 2. Cloud Storage

### Giới thiệu

Google Cloud Storage (GCS) là object storage service, đóng vai trò là storage layer cho data lake.

### Storage Classes

1. **Standard**
   - High availability và performance
   - Phù hợp cho dữ liệu truy cập thường xuyên

2. **Nearline**
   - Lower cost, phù hợp cho dữ liệu truy cập ít hơn 1 lần/tháng

3. **Coldline**
   - Chi phí thấp hơn, phù hợp cho dữ liệu truy cập ít hơn 1 lần/quarter

4. **Archive**
   - Chi phí thấp nhất, phù hợp cho long-term archival

### Tính năng quan trọng

#### 2.1. Lifecycle Management
- Automatic transition giữa storage classes
- Automatic deletion
- Cost optimization

#### 2.2. Object Versioning
- Version history cho objects
- Data protection và recovery

#### 2.3. Requester Pays
- Bucket owner có thể yêu cầu requester trả phí access

### Best Practices

- Sử dụng naming conventions cho buckets
- Enable versioning cho production data
- Cấu hình lifecycle policies
- Sử dụng Cloud CDN cho performance

## 3. BigLake

### Giới thiệu

BigLake là một unified storage layer cho phép query dữ liệu trong open formats mà không cần copy data.

### Tính năng chính

#### 3.1. Open Formats Support
- **Apache Iceberg**: Table format với ACID transactions
- **Delta Lake**: Transactional data lake storage
- **Apache Hudi**: Incremental data processing

#### 3.2. Multi-Cloud Support
- Query data từ AWS S3
- Query data từ Azure ADLS
- Unified interface across clouds

#### 3.3. Fine-Grained Access Control
- Column-level security
- Row-level security với dynamic filters
- Integration với IAM và external authorization

#### 3.4. Performance Optimization
- Automatic partitioning
- Metadata caching
- Predicate pushdown

### Architecture

```
┌─────────────┐
│  BigQuery   │
│   Engine    │
└──────┬──────┘
       │
┌──────▼─────────────────────────────────┐
│         BigLake Layer                  │
│  ┌──────────┐  ┌──────────┐          │
│  │ Iceberg  │  │  Delta   │          │
│  │  Tables  │  │  Tables  │          │
│  └──────────┘  └──────────┘          │
│  ┌────────────────────────────────┐  │
│  │   Access Control & Security    │  │
│  └────────────────────────────────┘  │
└──────┬─────────────────────────────────┘
       │
┌──────▼─────────────────────────────────┐
│         Storage Layer                  │
│  GCS | S3 | ADLS | ...                │
└────────────────────────────────────────┘
```

## 4. Dataplex

### Giới thiệu

Dataplex là intelligent data fabric giúp quản lý, monitor, và govern dữ liệu trên Google Cloud.

### Tính năng chính

#### 4.1. Data Discovery
- Automatic data discovery
- Metadata management
- Data catalog
- Search và exploration

#### 4.2. Data Quality
- Data profiling
- Quality checks
- Data validation rules
- Quality dashboards

#### 4.3. Data Lineage
- End-to-end data lineage
- Impact analysis
- Dependency tracking

#### 4.4. Governance
- Policy management
- Access control
- Data classification
- Compliance tracking

### Core Concepts

1. **Lake**: Logical container cho dữ liệu liên quan
2. **Zone**: Khu vực dữ liệu với cùng data classification
3. **Asset**: Logical representation của data source
4. **Task**: Data processing jobs

## 5. Dataproc

### Giới thiệu

Dataproc là managed service cho Apache Spark và Hadoop, hữu ích cho ETL/ELT workloads trong lakehouse.

### Tính năng

- Managed Spark clusters
- Auto-scaling
- Integration với GCS
- Preemptible VMs để giảm cost

### Use Cases

- Large-scale data processing
- ETL/ELT pipelines
- ML model training
- Data transformation

## 6. Dataflow

### Giới thiệu

Dataflow là serverless service cho stream và batch processing, built trên Apache Beam.

### Tính năng

- Serverless execution
- Auto-scaling
- Exactly-once processing
- Integration với Pub/Sub, BigQuery, GCS

### Use Cases

- Real-time data processing
- Stream analytics
- ETL pipelines
- Event-driven processing

## 7. Integration Services

### Pub/Sub

- Real-time messaging
- Event streaming
- Integration với Dataflow

### Cloud Functions & Cloud Run

- Serverless compute
- Event-driven processing
- API endpoints

### Cloud Run Jobs

- Serverless job execution
- Python scripts execution
- Containerized workloads
- Scheduled với Cloud Scheduler

### Cloud Scheduler

- Cron-based scheduling
- HTTP triggers cho Cloud Run Jobs
- Timezone support
- Retry configuration

## Architecture Integration

```
┌────────────────────────────────────────────────────┐
│              Data Sources                           │
│  (APIs, Databases, Files, Streams)                 │
└───────────────┬────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         Ingestion Layer                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Pub/Sub  │  │ Dataflow │  │ Dataproc │        │
│  └──────────┘  └──────────┘  └──────────┘        │
└───────────────┬────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         Storage Layer (Cloud Storage)              │
│  ┌──────────────────────────────────────────────┐ │
│  │  Raw Zone | Cleaned Zone | Curated Zone      │ │
│  └──────────────────────────────────────────────┘ │
└───────────────┬────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         BigLake Layer                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Iceberg  │  │  Delta   │  │   Hudi   │        │
│  └──────────┘  └──────────┘  └──────────┘        │
└───────────────┬────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         Analytics Layer (BigQuery)                 │
│  ┌──────────────────────────────────────────────┐ │
│  │  Native Tables | External Tables | BigLake   │ │
│  └──────────────────────────────────────────────┘ │
└───────────────┬────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         Governance Layer (Dataplex)                │
│  (Catalog, Quality, Lineage, Security)             │
└────────────────────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────┐
│         Consumption Layer                          │
│  (BI Tools, ML, Applications)                      │
└────────────────────────────────────────────────────┘
```

## Tài liệu tham khảo

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [BigLake Documentation](https://cloud.google.com/bigquery/docs/biglake-intro)
- [Dataplex Documentation](https://cloud.google.com/dataplex/docs)
- [Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [Dataflow Documentation](https://cloud.google.com/dataflow/docs)

