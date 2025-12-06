# Dataplex: Data Governance cho Lakehouse

## Giới thiệu

Dataplex là intelligent data fabric của Google Cloud, cung cấp unified data management, governance, và analytics cho lakehouse architecture.

## Core Concepts

### 1. Lake

**Lake** là logical container tổ chức dữ liệu liên quan trong một domain.

**Đặc điểm**:
- Logical organization unit
- Can span multiple storage systems
- Central governance point

**Example**:
```
Lake: enterprise-data-lake
├── Zone: raw-data-zone
├── Zone: curated-data-zone
└── Zone: analytics-zone
```

### 2. Zone

**Zone** là khu vực dữ liệu với cùng data classification và governance policies.

**Zone Types**:
- **Raw**: Unprocessed data
- **Curated**: Processed, trusted data
- **Managed**: Fully managed by Dataplex

**Example**:
```yaml
Zone: bronze-zone
  Type: RAW
  Location: us-central1
  Storage: gs://data-lake-bronze
  Policies:
    - Data retention: 90 days
    - Access: Data Engineers only
```

### 3. Asset

**Asset** là logical representation của data source (tables, buckets, datasets).

**Asset Types**:
- BigQuery datasets
- Cloud Storage buckets
- BigTable instances
- External data sources

**Example**:
```yaml
Asset: orders-bronze
  Type: STORAGE_BUCKET
  Resource: gs://data-lake-bronze/orders
  Schema: Auto-discovered
  Metadata: Business descriptions
```

### 4. Task

**Task** là data processing jobs (Spark, Notebooks, SQL scripts).

**Task Types**:
- Data Quality
- Data Transformation
- Data Discovery
- Custom tasks

## Tính năng chính

### 1. Data Discovery

#### Automatic Discovery

- Auto-discover tables và schemas
- Extract metadata
- Build data catalog
- Update automatically

#### Search và Exploration

- Full-text search
- Filter by tags, labels
- Browse by hierarchy
- View data previews

#### Data Profiling

- Automatic profiling
- Statistics generation
- Data quality insights
- Schema detection

### 2. Metadata Management

#### Metadata Catalog

```yaml
Table: orders
  Schema:
    - order_id: STRING
    - order_date: DATE
    - amount: FLOAT64
  Business Metadata:
    Description: Customer order transactions
    Owner: data-engineering-team
    Classification: PII
    Tags: [sales, transactions, production]
```

#### Lineage Tracking

- End-to-end lineage
- Impact analysis
- Dependency tracking
- Visual lineage graphs

#### Schema Management

- Version schemas
- Track schema changes
- Schema validation
- Schema registry

### 3. Data Quality

#### Quality Rules

```sql
-- Completeness rule
CREATE QUALITY RULE orders_completeness
AS
SELECT 
  COUNT(*) as total_rows,
  COUNT(order_id) as non_null_order_id,
  COUNT(order_id) / COUNT(*) as completeness_score
FROM orders
WHERE completeness_score < 0.95;
```

#### Quality Dimensions

- **Completeness**: Required fields present
- **Accuracy**: Data values correct
- **Consistency**: Consistent across sources
- **Timeliness**: Data freshness
- **Validity**: Meets business rules
- **Uniqueness**: No duplicates

#### Quality Monitoring

- Scheduled quality checks
- Real-time validation
- Quality dashboards
- Alert on failures

### 4. Governance

#### Policy Management

```yaml
Policy: data-retention-policy
  Scope: bronze-zone
  Rule: Delete data older than 90 days
  Action: Lifecycle policy

Policy: access-policy
  Scope: sensitive-data
  Rule: Only authorized users
  Action: IAM enforcement
```

#### Access Control

- Fine-grained permissions
- Role-based access
- Attribute-based access
- Integration với IAM

#### Data Classification

- Automatic classification
- PII detection
- Sensitive data tagging
- Compliance tracking

### 5. Data Processing

#### Dataproc Templates

- Pre-built Spark templates
- Data quality templates
- Transformation templates
- Custom templates

#### Notebook Execution

- Jupyter notebooks
- Scheduled execution
- Parameterized notebooks
- Results storage

## Implementation Guide

### 1. Setup Dataplex

```bash
# Enable Dataplex API
gcloud services enable dataplex.googleapis.com

# Create lake
gcloud dataplex lakes create enterprise-data-lake \
  --location=us-central1 \
  --description="Enterprise data lake"
```

### 2. Create Zones

```bash
# Bronze zone (Raw data)
gcloud dataplex zones create bronze-zone \
  --lake=enterprise-data-lake \
  --location=us-central1 \
  --resource-location-type=SINGLE_REGION \
  --type=RAW \
  --discovery-enabled

# Silver zone (Curated data)
gcloud dataplex zones create silver-zone \
  --lake=enterprise-data-lake \
  --location=us-central1 \
  --resource-location-type=SINGLE_REGION \
  --type=CURATED \
  --discovery-enabled
```

### 3. Register Assets

```bash
# Register BigQuery dataset
gcloud dataplex assets create orders-dataset \
  --lake=enterprise-data-lake \
  --zone=silver-zone \
  --location=us-central1 \
  --resource-type=BIGQUERY_DATASET \
  --resource-name=projects/my-project/datasets/orders \
  --discovery-enabled

# Register Storage bucket
gcloud dataplex assets create orders-bucket \
  --lake=enterprise-data-lake \
  --zone=bronze-zone \
  --location=us-central1 \
  --resource-type=STORAGE_BUCKET \
  --resource-name=projects/my-project/buckets/data-lake-bronze \
  --discovery-enabled
```

### 4. Create Quality Rules

```python
# Python example
from google.cloud import dataplex_v1

client = dataplex_v1.DataQualityServiceClient()

rule = dataplex_v1.DataQualityRule(
    name="orders_completeness",
    dimension="COMPLETENESS",
    sql_assertion="""
    SELECT COUNT(*) as total,
           SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) as non_null
    FROM orders
    WHERE non_null / total < 0.95
    """,
    threshold=0.95
)
```

### 5. Setup Tasks

```bash
# Create data quality task
gcloud dataplex tasks create data-quality-orders \
  --lake=enterprise-data-lake \
  --location=us-central1 \
  --trigger-type=ON_DEMAND \
  --execution-service=DATA_QUALITY \
  --spark-main-class=com.google.cloud.dataplex.tasks.DataQualityTask \
  --spark-python-script-file=gs://scripts/data-quality.py
```

## Best Practices

### 1. Organization Structure

```
Lake: enterprise-data-lake
├── Zone: raw-zone (RAW)
│   ├── Asset: source1-bucket
│   └── Asset: source2-bucket
├── Zone: curated-zone (CURATED)
│   ├── Asset: orders-dataset
│   └── Asset: customers-dataset
└── Zone: analytics-zone (CURATED)
    └── Asset: analytics-dataset
```

### 2. Naming Conventions

- **Lakes**: `{domain}-data-lake`
- **Zones**: `{layer}-zone` (e.g., `bronze-zone`, `silver-zone`)
- **Assets**: `{entity}-{type}` (e.g., `orders-dataset`, `customers-bucket`)

### 3. Metadata Management

- Add business descriptions
- Tag assets appropriately
- Maintain data lineage
- Document schemas

### 4. Quality Rules

- Start with critical rules
- Set appropriate thresholds
- Monitor quality trends
- Alert on failures

### 5. Governance

- Define clear policies
- Enforce consistently
- Regular audits
- Update policies as needed

## Integration với Services khác

### BigQuery

- Automatic discovery
- Query from Dataplex
- Metadata sync
- Quality checks

### Cloud Storage

- Bucket registration
- Schema discovery
- File-level metadata
- Lifecycle policies

### Dataflow/Dataproc

- Task execution
- Quality checks
- Transformation tasks
- Monitoring

### Cloud Run Jobs

- Execute Python scripts
- Containerized jobs
- Scheduled với Cloud Scheduler
- Dependency có thể handle trong scripts hoặc Cloud Scheduler chains

## Monitoring và Alerting

### Key Metrics

- Data quality scores
- Discovery status
- Task execution status
- Metadata freshness

### Alerts

- Quality failures
- Discovery errors
- Task failures
- Policy violations

## Tài liệu tham khảo

- [Dataplex Documentation](https://cloud.google.com/dataplex/docs)
- [Dataplex Best Practices](https://cloud.google.com/dataplex/docs/best-practices)
- [Data Quality Guide](https://cloud.google.com/dataplex/docs/data-quality)
- [Governance Patterns](https://cloud.google.com/dataplex/docs/governance)

