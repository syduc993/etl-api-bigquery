# Project Plan: Nhanh.vn to Google Cloud Lakehouse ETL

## 1. Project Overview
Build a robust ETL (Extract, Transform, Load) pipeline to move data from Nhanh.vn API to a Google Cloud Lakehouse architecture. The system will follow the Medallion Architecture (Bronze, Silver, Gold) to ensure data quality and accessibility.

**Key Constraints from Nhanh API:**
- API chỉ hỗ trợ lấy dữ liệu trong khoảng **31 ngày** mỗi lần query
- Rate limit: **150 requests / 30 giây** (per appId + businessId + API URL)
- Pagination: `paginator.next` là object/array, không phải string
- Endpoint: `https://pos.open.nhanh.vn/v3.0/...` (POST method)

## 2. Architecture Design
Based on the `docs/nhanh-api` and `docs/lakehouse-google-cloud` documentation.

### 2.1. Layers (Medallion Architecture)
*   **Bronze Layer (Raw)**:
    *   **Storage**: Google Cloud Storage (GCS) buckets.
    *   **Format**: JSON (as received from API) - preserve exact response for replay capability.
    *   **Strategy**: Append-only. Save response payloads exactly as received to allow full history replay.
    *   **Partitioning**: 
        *   **Recommended**: `{entity}/year={YYYY}/month={MM}/day={DD}/timestamp_{timestamp}.json` (day-level)
        *   **Alternative**: `{entity}/year={YYYY}/month={MM}/timestamp_{timestamp}.json` (month-level)
        *   **Decision factors**: 
            *   Use **day-level** nếu: data volume >1GB/day per entity, frequent daily queries, nhiều extraction runs per day
            *   Use **month-level** nếu: data volume <100MB/day, queries thường monthly/weekly, muốn đơn giản hóa
        *   **Note**: Có thể bắt đầu với month-level và nâng cấp lên day-level sau nếu cần
    *   **Lifecycle**: Auto-transition to Nearline after 90 days, Coldline after 365 days.
*   **Silver Layer (Cleaned & Enriched)**:
    *   **Storage**: BigLake tables (Iceberg format) hoặc BigQuery Native tables.
    *   **Format**: Parquet (columnar) for optimal query performance.
    *   **Transformation**: 
        *   Deduplication based on `id` field
        *   Type casting (Int, Date, Float, String)
        *   Schema validation
        *   Unpacking nested JSON (e.g., flattening `products` array in bills)
        *   Null handling và data quality checks
    *   **Data Quality**: Dataplex quality rules (completeness, accuracy, consistency)
*   **Gold Layer (Curated/Business Aggregates)**:
    *   **Storage**: BigQuery Native tables/views (optimized for analytics).
    *   **Transformation**: Business logic, aggregations (e.g., Daily Revenue, Customer Lifetime Value).
    *   **Partitioning**: By date for time-series queries
    *   **Clustering**: By business keys (customer_id, product_id) for join performance

### 2.2. Data Flow
1.  **Ingestion (Extract)**: Python scripts (Cloud Run Jobs) call Nhanh.vn API.
    *   *Key APIs*: 
        *   `/v3.0/bill/list` (Invoices - giới hạn 31 ngày)
        *   `/v3.0/product/list` (Products)
        *   `/v3.0/customer/list` (Customers)
    *   *Mechanism*: 
        *   Pagination loop handling `paginator.next` (object/array format)
        *   Incremental extraction: Track `updatedAt` watermark để chỉ lấy data mới
        *   Date range splitting: Chia 31 ngày thành các batch nhỏ hơn nếu cần
    *   *Rate Limit Handling*: 
        *   Monitor `ERR_429` responses
        *   Parse `lockedSeconds` và `unlockedAt` từ error response
        *   Exponential backoff với jitter
        *   Respect 150 req/30s limit với token bucket algorithm
    *   *Error Handling*:
        *   Retry logic cho transient errors
        *   Log và alert cho persistent failures
        *   Idempotent requests (safe to retry)
2.  **Storage (Load)**: 
    *   Save raw JSON responses to GCS bucket với partitioning:
        *   Day-level: `gs://<project>-bronze/{entity}/year={YYYY}/month={MM}/day={DD}/timestamp_{timestamp}.json`
        *   Month-level: `gs://<project>-bronze/{entity}/year={YYYY}/month={MM}/timestamp_{timestamp}.json`
    *   Include metadata: ingestion timestamp, API request params, response status
    *   **Partitioning decision**: Xem Section 6.3.1 cho guidelines
3.  **Processing (Transform)**:
    *   **Bronze → Silver**: 
        *   Load JSON từ GCS vào BigQuery External tables hoặc BigLake (Parquet)
        *   SQL transformations với dbt hoặc BigQuery SQL scripts
        *   Data quality validation với Dataplex
    *   **Silver → Gold**: 
        *   SQL aggregations và business logic
        *   Materialized views cho common queries
        *   Incremental updates với MERGE statements

## 3. Technology Stack
*   **Language**: Python 3.11+
*   **Cloud Platform**: Google Cloud Platform (GCP)
*   **Storage**: 
    *   Cloud Storage (GCS) - Bronze layer
    *   BigQuery - Silver/Gold layers
    *   BigLake - Open formats (Iceberg) cho Silver layer
*   **Compute**: Cloud Run Jobs (for batch extraction - recommended)
*   **Orchestration**: Cloud Scheduler (CRON) cho scheduled jobs
*   **Data Governance**: Dataplex (data catalog, quality, lineage)
*   **Secrets Management**: Secret Manager (API keys, tokens)
*   **Monitoring**: Cloud Logging, Cloud Monitoring, Alerting
*   **Infrastructure as Code**: Terraform (Recommended)

## 4. Proposed Folder Structure
This structure will be created in the new project folder.

```text
nhanh-etl-lakehouse/
├── src/
│   ├── config.py           # Configuration (API keys, constraints, GCP settings)
│   ├── main.py             # Entry point cho Cloud Run Jobs
│   ├── extractors/         # Logic to pull data from Nhanh API
│   │   ├── __init__.py
│   │   ├── base.py         # Base extractor (auth, rate limit, paging, error handling)
│   │   ├── bill.py         # Bill extractor (handle 31-day limit)
│   │   ├── product.py      # Product extractor
│   │   └── customer.py     # Customer extractor
│   ├── loaders/            # Logic to save data to GCS/BQ
│   │   ├── __init__.py
│   │   ├── gcs_loader.py   # Upload to GCS với partitioning
│   │   └── watermark.py    # Track extraction watermark (updatedAt)
│   ├── transformers/       # Data transformation logic
│   │   ├── __init__.py
│   │   ├── bronze_to_silver.py  # Clean và transform
│   │   └── silver_to_gold.py    # Aggregations
│   └── utils/              # Logging, helpers, common functions
│       ├── __init__.py
│       ├── logging.py      # Structured logging
│       └── exceptions.py   # Custom exceptions
├── sql/                    # SQL transformation scripts
│   ├── bronze/             # Bronze layer schemas
│   ├── silver/             # Silver transformations
│   └── gold/               # Gold aggregations
├── infrastructure/         # Terraform or setup scripts
│   ├── terraform/          # IaC definitions
│   └── scripts/            # Setup scripts
├── dataplex/               # Dataplex configuration
│   ├── lakes/              # Lake definitions
│   ├── zones/              # Zone definitions
│   └── quality_rules/     # Data quality rules
├── tests/                    # Unit and integration tests
│   ├── unit/
│   ├── integration/
│   └── fixtures/          # Test data
├── docker/                # Dockerfiles
│   └── Dockerfile
├── requirements.txt        # Python dependencies
├── .env.example           # Environment variables template
└── README.md
```

## 5. Implementation Steps

### Phase 1: Setup & Bronze Ingestion
1.  **Environment Setup**: 
    *   Create GCP Project
    *   Enable APIs: BigQuery, Cloud Storage, Cloud Run, Secret Manager, Dataplex, Cloud Scheduler
    *   Create GCS Buckets: `{project}-bronze`, `{project}-silver` (với versioning enabled)
    *   Create BigQuery Datasets: `bronze`, `silver`, `gold`
    *   Setup Dataplex Lake và Zones (bronze-zone, silver-zone, gold-zone)
    *   Create Service Account với appropriate IAM roles
    *   Store secrets in Secret Manager (accessToken, appId, businessId)

2.  **Base Extractor (`NhanhApiClient`)**:
    *   Authentication: appId, businessId (query params), accessToken (Authorization header)
    *   Rate limiting: 
        *   Token bucket algorithm (150 req/30s)
        *   Handle `ERR_429` với exponential backoff
        *   Parse `lockedSeconds` và `unlockedAt` từ error response
        *   Sleep until `unlockedAt` khi bị rate limited
    *   Pagination: 
        *   Handle `paginator.next` as object/array (not string)
        *   Loop until `next` is null or empty
        *   Track pagination state for resume capability
    *   Error handling:
        *   Retry logic cho transient errors (5xx, network errors)
        *   Log và alert cho persistent failures
        *   Validate response `code: 1` (success) vs `code: 0` (failed)
    *   Date range handling:
        *   Split 31-day windows thành smaller batches nếu cần
        *   Track `updatedAtFrom` và `updatedAtTo` for incremental extraction

3.  **Entity Extractors**:
    *   `fetch_bills`: 
        *   Handle 31-day limit constraint
        *   Support filters: `fromDate`, `toDate`, `modes`, `type`, `customerId`
        *   Incremental: Use `updatedAtFrom`/`updatedAtTo` filters
    *   `fetch_products`: Full sync hoặc incremental based on `updatedAt`
    *   `fetch_customers`: Full sync hoặc incremental based on `updatedAt`

4.  **GCS Loader**:
    *   Upload JSON blobs với partitioning: `{entity}/year={YYYY}/month={MM}/day={DD}/timestamp_{timestamp}.json`
    *   Include metadata file: `_metadata/{timestamp}.json` (request params, response status, record count)
    *   Idempotent: Check if file exists before upload (skip if exists)
    *   Compression: Gzip JSON files để giảm storage cost

5.  **Watermark Tracking**:
    *   Store last successful extraction timestamp per entity
    *   Use BigQuery table hoặc GCS metadata để track
    *   Enable incremental extraction (chỉ lấy data mới)

### Phase 2: Silver Layer (Data Warehousing)
1.  **Schema Definition**: 
    *   Define BigQuery schemas for `Bill`, `Product`, `Customer` based on API response structure
    *   Handle nested structures (flatten `products` array in bills)
    *   Define data types: INT64, FLOAT64, STRING, DATE, TIMESTAMP, JSON

2.  **Load Job (Bronze → Silver)**:
    *   Create BigQuery External Table pointing to GCS JSON files
    *   Hoặc use BigLake table với Parquet format
    *   Incremental load: Process only new files since last run
    *   Use BigQuery Load Jobs hoặc Python script với `google-cloud-bigquery`

3.  **Transformation Logic**:
    *   SQL transformations (dbt hoặc BigQuery SQL scripts):
        *   `CAST` fields to correct types (INT64, DATE, FLOAT64, STRING)
        *   Handle `NULL`s (coalesce, default values)
        *   Deduplicate records based on `id` (use `ROW_NUMBER()` hoặc `MERGE`)
        *   Flatten nested JSON (UNNEST `products` array)
        *   Data validation: Range checks, format validation
    *   Create BigLake Iceberg table cho Silver layer (optional, for open format support)

4.  **Data Quality với Dataplex**:
    *   Define quality rules:
        *   Completeness: Required fields not null (>95%)
        *   Accuracy: Data format validation
        *   Consistency: Cross-table validation
        *   Uniqueness: No duplicate IDs
    *   Schedule quality checks
    *   Alert on quality failures

### Phase 3: Gold Layer (Business Aggregates)
1.  **Aggregation Logic**:
    *   Create BigQuery Native tables (optimized for analytics)
    *   SQL aggregations:
        *   Daily Revenue Summary
        *   Customer Lifetime Value
        *   Product Sales Metrics
        *   Inventory Analytics
    *   Partition by date, cluster by business keys

2.  **Materialized Views**:
    *   Pre-aggregate common queries
    *   Auto-refresh với incremental updates

### Phase 4: Automation & Monitoring
1.  **Scheduling**:
    *   Deploy extractors as Cloud Run Jobs
    *   Cloud Scheduler triggers:
        *   Bronze extraction: Every 30 minutes (hoặc hourly)
        *   Silver transformation: After Bronze completion (delay 15 mins)
        *   Gold aggregation: After Silver completion (delay 15 mins)
    *   Handle job dependencies: Use Cloud Scheduler delays hoặc Pub/Sub triggers

2.  **Logging**:
    *   Structured logging với Cloud Logging
    *   Log levels: INFO (progress), WARNING (retries), ERROR (failures)
    *   Include: Job name, entity type, record counts, execution time, errors

3.  **Monitoring & Alerting**:
    *   Cloud Monitoring metrics:
        *   Job execution status (success/failure)
        *   Records processed per run
        *   API rate limit hits
        *   Data quality scores
        *   Pipeline latency
    *   Alert policies:
        *   Job failure (immediate alert)
        *   Data quality below threshold
        *   Rate limit exceeded frequently
        *   Pipeline latency spike
    *   Dashboard: GCP Monitoring dashboard cho pipeline health

4.  **Error Recovery**:
    *   Automatic retries với exponential backoff
    *   Dead letter queue cho failed records
    *   Manual re-run capability
    *   Data validation reports

## 6. Key Considerations

### 6.1. API Constraints
*   **31-Day Limit**: API chỉ hỗ trợ query tối đa 31 ngày. Cần:
    *   Track watermark (`updatedAt`) để incremental extraction
    *   Split historical backfill thành 31-day chunks
    *   Schedule frequent runs (30 mins - 1 hour) để không miss data
*   **Rate Limiting**: 
    *   150 requests / 30 seconds per (appId + businessId + API URL)
    *   Implement token bucket algorithm
    *   Handle `ERR_429` gracefully với exponential backoff
    *   Monitor rate limit usage để optimize scheduling
*   **Pagination**: 
    *   `paginator.next` là object/array, không phải string
    *   Must pass exact structure từ response vào next request
    *   Track pagination state để resume on failure

### 6.2. Data Management
*   **Secrets**: 
    *   Store `accessToken`, `appId`, `businessId` in GCP Secret Manager
    *   Rotate tokens periodically
    *   Use service account với least privilege IAM roles
*   **Idempotency**: 
    *   Bronze: Check file existence before upload (skip duplicates)
    *   Silver: Use `MERGE` statements với `id` as key
    *   Gold: Use `MERGE` hoặc `INSERT ... ON CONFLICT` for upserts
    *   Re-running jobs should not duplicate data
*   **Incremental Processing**:
    *   Track `updatedAt` watermark per entity
    *   Only process new/changed records
    *   Support backfill mode for historical data

### 6.3. Cost Optimization

#### 6.3.1. Partitioning Strategy Decision

**Day-level partitioning** (`year={YYYY}/month={MM}/day={DD}`):
*   **Use khi**:
    *   Data volume >1GB/day per entity
    *   Frequent daily queries (filter by specific date)
    *   Nhiều extraction runs per day (30 mins - hourly)
    *   Cần query performance tốt khi filter theo ngày
    *   Có nhiều files mỗi ngày (>10 files/day)
*   **Lợi ích**:
    *   Better query performance (partition pruning)
    *   Dễ dàng delete/archive data theo ngày
    *   Parallel processing tốt hơn
*   **Nhược điểm**:
    *   Phức tạp hơn trong implementation
    *   Nhiều partitions hơn (365 partitions/year)

**Month-level partitioning** (`year={YYYY}/month={MM}`):
*   **Use khi**:
    *   Data volume <100MB/day per entity
    *   Queries thường monthly/weekly (ít khi filter theo ngày cụ thể)
    *   Ít extraction runs (daily hoặc ít hơn)
    *   Muốn đơn giản hóa structure
    *   Số lượng files ít (<10 files/month)
*   **Lợi ích**:
    *   Đơn giản hơn
    *   Ít partitions (12 partitions/year)
    *   Dễ maintain
*   **Nhược điểm**:
    *   Query performance kém hơn khi filter theo ngày
    *   Phải scan toàn bộ tháng

**Recommendation**:
*   **Bắt đầu với month-level** cho simplicity
*   **Monitor data volume và query patterns** trong 1-2 tháng
*   **Nâng cấp lên day-level** nếu:
    *   Data volume tăng >100MB/day
    *   Query patterns thay đổi (cần daily filtering)
    *   Performance issues xuất hiện

*   **Storage**:
    *   GCS Lifecycle policies: Bronze → Nearline (90 days) → Coldline (365 days)
    *   Compress JSON files (gzip) before upload
    *   Use Parquet format cho Silver layer (better compression)
    *   Delete old Bronze data sau khi đã load vào Silver (optional, based on retention policy)
*   **BigQuery**:
    *   Partition tables by date để reduce scanned data
    *   Cluster tables by business keys (customer_id, product_id)
    *   Use query result caching
    *   Materialized views cho common aggregations
    *   Right-size slots based on workload
*   **Compute**:
    *   Right-size Cloud Run Jobs (memory/CPU)
    *   Use incremental processing để giảm processing time
    *   Schedule jobs during off-peak hours nếu có thể

### 6.4. Data Quality
*   **Validation Rules**:
    *   Schema validation (required fields, data types)
    *   Business rule validation (amount > 0, dates in range)
    *   Referential integrity (customer_id exists)
    *   Completeness checks (>95% non-null for critical fields)
*   **Monitoring**:
    *   Track quality scores over time
    *   Alert on quality degradation
    *   Data profiling với Dataplex
*   **Error Handling**:
    *   Dead letter queue cho invalid records
    *   Retry logic cho transient errors
    *   Manual review process cho persistent failures

### 6.5. Security
*   **Access Control**:
    *   Service account với least privilege
    *   IAM roles: `roles/storage.objectAdmin`, `roles/bigquery.dataEditor`
    *   Column-level security cho PII data (policy tags)
    *   Audit logging enabled
*   **Encryption**:
    *   GCS: Default encryption at rest
    *   BigQuery: Default encryption
    *   In transit: TLS/SSL
*   **Compliance**:
    *   Data retention policies
    *   PII data classification
    *   Access audit logs

### 6.6. Scalability
*   **Horizontal Scaling**:
    *   Cloud Run Jobs tự động scale
    *   Parallel processing cho multiple entities
    *   Partition data để enable parallel processing
*   **Performance**:
    *   Optimize API calls (batch requests nếu có thể)
    *   Use BigQuery streaming inserts cho real-time updates (nếu cần)
    *   Cache metadata queries

## 7. Implementation Checklist

### Phase 1: Foundation
*   [ ] Create GCP project và enable APIs
*   [ ] Setup GCS buckets với lifecycle policies
*   [ ] Create BigQuery datasets (bronze, silver, gold)
*   [ ] Setup Dataplex Lake và Zones
*   [ ] Create Service Account và IAM roles
*   [ ] Store secrets in Secret Manager
*   [ ] Initialize code repository structure

### Phase 2: Bronze Layer
*   [ ] Implement `NhanhApiClient` base class
    *   [ ] Authentication
    *   [ ] Rate limiting (token bucket)
    *   [ ] Error handling (ERR_429, retries)
    *   [ ] Pagination handling
*   [ ] Implement entity extractors
    *   [ ] `fetch_bills` (handle 31-day limit)
    *   [ ] `fetch_products`
    *   [ ] `fetch_customers`
*   [ ] Implement GCS loader với partitioning
*   [ ] Implement watermark tracking
*   [ ] Create Cloud Run Job cho extraction
*   [ ] Test với sample data

### Phase 3: Silver Layer
*   [ ] Define BigQuery schemas
*   [ ] Create BigQuery External tables (hoặc BigLake)
*   [ ] Implement transformation SQL
    *   [ ] Type casting
    *   [ ] Deduplication
    *   [ ] Flatten nested JSON
    *   [ ] Data validation
*   [ ] Setup Dataplex quality rules
*   [ ] Create Cloud Run Job cho transformation
*   [ ] Test data quality

### Phase 4: Gold Layer
*   [ ] Design aggregation tables
*   [ ] Implement SQL aggregations
*   [ ] Create materialized views
*   [ ] Test query performance

### Phase 5: Automation
*   [ ] Setup Cloud Scheduler jobs
*   [ ] Configure monitoring và alerting
*   [ ] Create dashboards
*   [ ] Document runbooks

### Phase 6: Production Readiness
*   [ ] Load testing
*   [ ] Error recovery testing
*   [ ] Security audit
*   [ ] Cost optimization review
*   [ ] Documentation complete

## 8. Risk Mitigation

### API Limitations
*   **Risk**: 31-day query limit có thể miss data nếu job fails
*   **Mitigation**: 
    *   Frequent scheduling (30 mins)
    *   Watermark tracking để resume
    *   Alert on missed windows
    *   Manual backfill capability

### Rate Limiting
*   **Risk**: Exceed rate limit, jobs fail
*   **Mitigation**:
    *   Token bucket algorithm
    *   Exponential backoff
    *   Monitor rate limit usage
    *   Optimize API call frequency

### Data Quality
*   **Risk**: Bad data propagates to Gold layer
*   **Mitigation**:
    *   Quality rules với Dataplex
    *   Validation at each layer
    *   Alert on quality failures
    *   Dead letter queue

### Cost Overruns
*   **Risk**: High BigQuery query costs
*   **Mitigation**:
    *   Partitioning và clustering
    *   Incremental processing
    *   Query optimization
    *   Cost monitoring và budgets

## 9. Success Metrics

*   **Data Freshness**: Data available in Gold layer within 1 hour of source update
*   **Data Quality**: >95% quality score across all entities
*   **Pipeline Reliability**: >99% job success rate
*   **Cost**: Stay within budget (monitor monthly)
*   **Performance**: Bronze → Silver → Gold completion within 30 minutes

## 10. Next Actions
*   [ ] Review và approve this plan
*   [ ] Setup GCP infrastructure (Phase 1)
*   [ ] Begin implementation of `NhanhApiClient` (Phase 2)
*   [ ] Create initial test data extraction
