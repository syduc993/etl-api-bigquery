# Tổng quan về Lakehouse trên Google Cloud

## Khái niệm Lakehouse

Lakehouse là một kiến trúc dữ liệu mới kết hợp những điểm mạnh của cả Data Lake và Data Warehouse:

### Đặc điểm chính

1. **Lưu trữ linh hoạt**
   - Hỗ trợ cả dữ liệu có cấu trúc (structured) và phi cấu trúc (unstructured)
   - Lưu trữ dữ liệu ở dạng gốc (raw format)
   - Cost-effective storage như data lake

2. **Hiệu năng truy vấn cao**
   - Query performance tương đương data warehouse
   - Hỗ trợ SQL và analytics workloads
   - Optimized query engine

3. **ACID Transactions**
   - Đảm bảo tính nhất quán dữ liệu
   - Hỗ trợ concurrent reads/writes
   - Schema evolution

4. **Open Formats**
   - Apache Iceberg
   - Delta Lake
   - Apache Hudi
   - Parquet, ORC, Avro

## Lợi ích của Lakehouse

### 1. Unified Architecture
- Một platform cho cả analytics và machine learning
- Không cần di chuyển dữ liệu giữa lake và warehouse
- Giảm complexity và cost

### 2. Flexibility
- Hỗ trợ nhiều loại workloads:
  - BI và reporting
  - Data science và ML
  - Real-time analytics
  - Data engineering

### 3. Cost Efficiency
- Storage cost thấp hơn data warehouse truyền thống
- Compute tách biệt khỏi storage
- Pay-as-you-go pricing

### 4. Open Standards
- Tránh vendor lock-in
- Tương thích với ecosystem tools
- Dễ dàng migration

## Google Cloud Lakehouse Approach

Google Cloud cung cấp một nền tảng lakehouse hoàn chỉnh với các tính năng:

### Core Components

1. **BigQuery Lakehouse**
   - BigQuery như một unified analytics platform
   - Hỗ trợ query dữ liệu trong Cloud Storage
   - BigLake tables cho open formats

2. **Cloud Storage**
   - Scalable object storage
   - Multi-regional và regional options
   - Lifecycle management

3. **BigLake**
   - Unified storage layer
   - Fine-grained access control
   - Hỗ trợ open formats (Iceberg, Delta, Hudi)
   - Multi-cloud support

4. **Dataplex**
   - Intelligent data fabric
   - Data discovery và catalog
   - Data quality và governance
   - Unified metadata management

### Key Features

#### 1. Multi-Cloud Lakehouse
- Query dữ liệu trên AWS S3, Azure ADLS
- Unified interface across clouds
- Consistent security và governance

#### 2. Open Formats Support
- **Apache Iceberg**: Table format với time travel, schema evolution
- **Delta Lake**: ACID transactions, versioning
- **Apache Hudi**: Incremental processing, upserts

#### 3. Serverless Processing
- BigQuery: Serverless SQL analytics
- Dataflow: Serverless stream/batch processing
- Dataproc Serverless: Managed Spark jobs

#### 4. Advanced Analytics
- Machine Learning tích hợp
- Real-time analytics
- BI tools integration

## Use Cases

### 1. Enterprise Data Platform
- Unified data platform cho toàn tổ chức
- Self-service analytics
- Data democratization

### 2. Modern Data Warehouse Replacement
- Migrate từ data warehouse truyền thống
- Giảm cost và complexity
- Tăng flexibility

### 3. Data Lake Modernization
- Nâng cấp data lake hiện có
- Thêm ACID transactions
- Cải thiện query performance

### 4. Multi-Cloud Data Strategy
- Centralized data management
- Avoid vendor lock-in
- Consistent governance

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│           Application Layer                      │
│  (BI Tools, ML, Applications, APIs)              │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         Analytics & Query Layer                  │
│  ┌────────────┐  ┌────────────┐  ┌──────────┐  │
│  │ BigQuery   │  │ Dataflow   │  │ Dataproc │  │
│  └────────────┘  └────────────┘  └──────────┘  │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         Unified Storage Layer (BigLake)          │
│  ┌────────────┐  ┌────────────┐  ┌──────────┐  │
│  │  Iceberg   │  │   Delta    │  │   Hudi   │  │
│  └────────────┘  └────────────┘  └──────────┘  │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         Storage Layer                            │
│  ┌────────────────────────────────────────────┐  │
│  │        Cloud Storage (GCS)                  │  │
│  │  (Multi-regional, Regional, Nearline)       │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│         Governance Layer (Dataplex)              │
│  (Data Catalog, Quality, Lineage, Security)      │
└──────────────────────────────────────────────────┘
```

## Next Steps

1. Đọc về [Các thành phần chính](./02-COMPONENTS.md)
2. Tìm hiểu [Kiến trúc và Patterns](./03-ARCHITECTURE.md)
3. Xem [Implementation Guide](./04-IMPLEMENTATION.md)

## Tài liệu tham khảo

- [Building a Data Lakehouse on Google Cloud Platform (White Paper)](https://services.google.com/fh/files/misc/building-a-data-lakehouse.pdf)
- [BigLake: Multi-Cloud Lakehouse](https://storage.googleapis.com/gweb-research2023-media/pubtools/pdf/b6e4bce752acf2f0eb27cf56bcca6ffdfc3db780.pdf)
- [Google Cloud BigQuery Documentation](https://cloud.google.com/bigquery/docs)

