# Implementation Guide: Xây dựng Lakehouse trên Google Cloud

## Bước 1: Planning và Design

### 1.1. Requirements Gathering

**Các câu hỏi cần trả lời**:
- Data sources nào cần tích hợp?
- Volume và velocity của data?
- Use cases và workloads?
- Compliance và security requirements?
- Budget và cost constraints?

### 1.2. Architecture Design

1. **Define Layers**
   - Bronze (Raw)
   - Silver (Cleaned)
   - Gold (Curated)

2. **Choose Formats**
   - Open formats: Iceberg, Delta, Hudi
   - Storage format: Parquet recommended

3. **Design Partitions**
   - Time-based partitioning
   - Business key partitioning

4. **Security Model**
   - IAM roles và permissions
   - Data classification
   - Access policies

## Bước 2: Setup Infrastructure

### 2.1. Create GCP Project

```bash
# Create project
gcloud projects create my-lakehouse-project

# Set project
gcloud config set project my-lakehouse-project

# Enable billing
gcloud billing projects link my-lakehouse-project \
  --billing-account=BILLING_ACCOUNT_ID
```

### 2.2. Enable APIs

```bash
# Enable required APIs
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataplex.googleapis.com \
  dataflow.googleapis.com \
  dataproc.googleapis.com \
  pubsub.googleapis.com \
  compute.googleapis.com
```

### 2.3. Create Storage Buckets

```bash
# Create buckets với naming convention
gsutil mb -p my-lakehouse-project -c STANDARD -l us-central1 \
  gs://my-lakehouse-bronze
gsutil mb -p my-lakehouse-project -c STANDARD -l us-central1 \
  gs://my-lakehouse-silver
gsutil mb -p my-lakehouse-project -c STANDARD -l us-central1 \
  gs://my-lakehouse-gold

# Enable versioning cho production
gsutil versioning set on gs://my-lakehouse-bronze
```

### 2.4. Setup BigQuery Dataset

```sql
-- Create datasets cho các layers
CREATE SCHEMA IF NOT EXISTS `my-project.bronze`
OPTIONS(
  description="Bronze layer - raw data"
);

CREATE SCHEMA IF NOT EXISTS `my-project.silver`
OPTIONS(
  description="Silver layer - cleaned data"
);

CREATE SCHEMA IF NOT EXISTS `my-project.gold`
OPTIONS(
  description="Gold layer - curated data"
);
```

## Bước 3: Setup Dataplex

### 3.1. Create Lake

```bash
# Create Dataplex lake
gcloud dataplex lakes create my-lakehouse-lake \
  --location=us-central1 \
  --description="Main lakehouse lake"
```

### 3.2. Create Zones

```bash
# Bronze zone
gcloud dataplex zones create bronze-zone \
  --lake=my-lakehouse-lake \
  --location=us-central1 \
  --resource-location-type=SINGLE_REGION \
  --type=RAW

# Silver zone
gcloud dataplex zones create silver-zone \
  --lake=my-lakehouse-lake \
  --location=us-central1 \
  --resource-location-type=SINGLE_REGION \
  --type=CURATED

# Gold zone
gcloud dataplex zones create gold-zone \
  --lake=my-lakehouse-lake \
  --location=us-central1 \
  --resource-location-type=SINGLE_REGION \
  --type=CURATED
```

### 3.3. Register Assets

```bash
# Register Bronze bucket
gcloud dataplex assets create bronze-storage \
  --lake=my-lakehouse-lake \
  --zone=bronze-zone \
  --location=us-central1 \
  --resource-type=STORAGE_BUCKET \
  --resource-name=projects/my-project/buckets/my-lakehouse-bronze \
  --discovery-enabled
```

## Bước 4: Implement Data Ingestion

### 4.1. Batch Ingestion với Dataflow

**Python Example**:

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_pipeline():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Source' >> beam.io.ReadFromText('source_path')
         | 'Transform' >> beam.Map(transform_function)
         | 'Write to Bronze' >> beam.io.WriteToParquet(
             'gs://my-lakehouse-bronze/orders',
             schema=orders_schema
         ))
```

**Deploy**:
```bash
python pipeline.py \
  --runner=DataflowRunner \
  --project=my-project \
  --region=us-central1 \
  --temp_location=gs://my-temp-bucket/temp
```

### 4.2. Streaming Ingestion

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run_streaming_pipeline():
    options = PipelineOptions(streaming=True)
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
             topic='projects/my-project/topics/orders'
         )
         | 'Parse JSON' >> beam.Map(json.loads)
         | 'Window' >> beam.WindowInto(beam.window.FixedWindows(60))
         | 'Write to Bronze' >> beam.io.WriteToParquet(
             'gs://my-lakehouse-bronze/orders',
             schema=orders_schema
         ))
```

### 4.3. CDC với Datastream

```bash
# Create connection profile
gcloud datastream connection-profiles create mysql-profile \
  --location=us-central1 \
  --type=mysql \
  --display-name="MySQL Source" \
  --mysql-hostname=SOURCE_HOST \
  --mysql-port=3306 \
  --mysql-username=USER \
  --mysql-password=PASSWORD

# Create stream
gcloud datastream streams create orders-stream \
  --location=us-central1 \
  --display-name="Orders CDC Stream" \
  --source=mysql-profile \
  --destination=GCS \
  --destination-gcs-bucket=my-lakehouse-bronze
```

## Bước 5: Data Transformation

### 5.1. Bronze to Silver

```python
# Dataflow pipeline để clean data
def clean_data(element):
    # Data quality checks
    if validate_schema(element):
        # Deduplication
        # Data type conversion
        # Null handling
        return cleaned_element
    return None

with beam.Pipeline() as p:
    (p
     | 'Read Bronze' >> beam.io.ReadFromParquet(
         'gs://my-lakehouse-bronze/orders/*'
     )
     | 'Clean Data' >> beam.Map(clean_data)
     | 'Write Silver' >> beam.io.WriteToParquet(
         'gs://my-lakehouse-silver/orders',
         schema=orders_schema
     ))
```

### 5.2. Create BigLake Tables

```sql
-- Create BigLake table cho Silver layer
CREATE OR REPLACE EXTERNAL TABLE `my-project.silver.orders`
WITH CONNECTION `my-project.us.biglake-connection`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-lakehouse-silver/orders/*'],
  hive_partition_uri_prefix = 'gs://my-lakehouse-silver/orders',
  require_hive_partition_filter = false
);

-- Create Iceberg table
CREATE TABLE `my-project.silver.orders_iceberg`
WITH (
  table_format = 'ICEBERG',
  catalog = 'iceberg_catalog',
  base_location = 'gs://my-lakehouse-silver/orders_iceberg'
);
```

### 5.3. Silver to Gold

```sql
-- Create Gold table với business logic
CREATE OR REPLACE TABLE `my-project.gold.daily_sales_summary`
PARTITION BY sale_date
CLUSTER BY product_category
AS
SELECT
  DATE(order_timestamp) as sale_date,
  product_category,
  COUNT(DISTINCT order_id) as order_count,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_order_value
FROM `my-project.silver.orders`
WHERE order_status = 'completed'
GROUP BY 1, 2;
```

## Bước 6: Setup Data Quality

### 6.1. Create Quality Rules trong Dataplex

```yaml
# data_quality_rule.yaml
quality_rules:
  - rule_id: "orders_not_null_check"
    dimension: "COMPLETENESS"
    threshold: 0.95
    sql_assertion: |
      SELECT COUNT(*) as total_rows,
             COUNT(order_id) as non_null_rows
      FROM `my-project.silver.orders`
      WHERE non_null_rows / total_rows < 0.95
```

### 6.2. Schedule Quality Checks

```bash
# Create Dataplex task
gcloud dataplex tasks create data-quality-task \
  --lake=my-lakehouse-lake \
  --location=us-central1 \
  --trigger-type=ON_DEMAND \
  --execution-service=DATA_QUALITY \
  --spark-main-class=com.google.cloud.dataplex.tasks.DataQualityTask
```

## Bước 7: Setup Orchestration với Cloud Run Jobs

### 7.1. Tạo Python Script cho Data Pipeline

**Example: Bronze Ingestion Script** (`scripts/ingest_bronze.py`):

```python
"""Script để ingest data vào Bronze layer"""
import os
import sys
import io
from google.cloud import storage
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_to_bronze():
    """Ingest data từ source vào Bronze layer"""
    try:
        project_id = os.getenv('GCP_PROJECT')
        source_bucket = os.getenv('SOURCE_BUCKET')
        bronze_bucket = os.getenv('BRONZE_BUCKET')
        
        if not all([project_id, source_bucket, bronze_bucket]):
            raise ValueError("Missing required environment variables")
        
        storage_client = storage.Client(project=project_id)
        
        # Download data từ source
        source_bucket_obj = storage_client.bucket(source_bucket)
        bronze_bucket_obj = storage_client.bucket(bronze_bucket)
        
        # Process và upload to bronze
        today = datetime.now().strftime('%Y%m%d')
        blobs = list(source_bucket_obj.list_blobs(prefix=f'data/{today}/'))
        
        if not blobs:
            logger.warning(f"No files found for {today}")
            return
        
        logger.info(f"Processing {len(blobs)} files for {today}")
        
        for blob in blobs:
            try:
                # Download data
                data = blob.download_as_bytes()
                logger.info(f"Downloaded {blob.name} ({len(data)} bytes)")
                
                # Upload to bronze với partition structure
                now = datetime.now()
                destination_path = (
                    f'orders/'
                    f'year={now.year}/'
                    f'month={now.month:02d}/'
                    f'day={now.day:02d}/'
                    f'{blob.name.split("/")[-1]}'
                )
                
                destination_blob = bronze_bucket_obj.blob(destination_path)
                destination_blob.upload_from_string(data, content_type=blob.content_type)
                logger.info(f"Uploaded to {destination_path}")
                
            except Exception as e:
                logger.error(f"Error processing {blob.name}: {str(e)}")
                continue
        
        logger.info(f"Successfully ingested {len(blobs)} files to bronze for {today}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    ingest_to_bronze()
```

**Example: Silver Transformation Script** (`scripts/transform_silver.py`):

```python
"""Script để transform data từ Bronze sang Silver"""
import os
import sys
import io
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_to_silver():
    """Transform và clean data từ Bronze sang Silver"""
    try:
        project_id = os.getenv('GCP_PROJECT')
        bronze_bucket = os.getenv('BRONZE_BUCKET')
        silver_bucket = os.getenv('SILVER_BUCKET')
        
        if not all([project_id, bronze_bucket, silver_bucket]):
            raise ValueError("Missing required environment variables")
        
        storage_client = storage.Client(project=project_id)
        bronze_bucket_obj = storage_client.bucket(bronze_bucket)
        silver_bucket_obj = storage_client.bucket(silver_bucket)
        
        # Get files từ bronze (process today's data)
        today = datetime.now()
        prefix = f'orders/year={today.year}/month={today.month:02d}/day={today.day:02d}/'
        blobs = list(bronze_bucket_obj.list_blobs(prefix=prefix))
        
        if not blobs:
            logger.warning(f"No files found in bronze for today")
            return
        
        logger.info(f"Processing {len(blobs)} files from bronze")
        
        for blob in blobs:
            if not blob.name.endswith(('.parquet', '.csv')):
                continue
                
            try:
                # Download và process
                data = blob.download_as_bytes()
                logger.info(f"Processing {blob.name}")
                
                # Read data
                if blob.name.endswith('.parquet'):
                    df = pd.read_parquet(io.BytesIO(data))
                else:
                    df = pd.read_csv(io.BytesIO(data))
                
                # Data quality checks
                initial_count = len(df)
                df = df.dropna(subset=['order_id'])  # Remove null order_ids
                df = df.drop_duplicates(subset=['order_id'])  # Deduplicate
                logger.info(f"Cleaned: {initial_count} -> {len(df)} rows")
                
                # Data type conversion
                if 'order_date' in df.columns:
                    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
                if 'amount' in df.columns:
                    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                    df = df[df['amount'] > 0]  # Filter invalid records
                
                if df.empty:
                    logger.warning(f"No valid records after cleaning for {blob.name}")
                    continue
                
                # Save to silver as Parquet
                table = pa.Table.from_pandas(df)
                output_path = f'orders_cleaned/{blob.name}'
                output_blob = silver_bucket_obj.blob(output_path)
                
                # Write to memory first
                buffer = io.BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)
                
                # Upload to GCS
                output_blob.upload_from_string(
                    buffer.read(),
                    content_type='application/octet-stream'
                )
                logger.info(f"Saved to {output_path}")
                
            except Exception as e:
                logger.error(f"Error processing {blob.name}: {str(e)}")
                continue
        
        logger.info("Successfully transformed data to silver")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    transform_to_silver()
```

**Example: Gold Aggregation Script** (`scripts/create_gold.py`):

```python
"""Script để tạo Gold layer tables"""
import os
import sys
from google.cloud import bigquery
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_gold_tables():
    """Tạo aggregated tables trong Gold layer"""
    try:
        project_id = os.getenv('GCP_PROJECT')
        
        if not project_id:
            raise ValueError("GCP_PROJECT environment variable is required")
        
        client = bigquery.Client(project=project_id)
        
        # Create daily sales summary
        query = """
        CREATE OR REPLACE TABLE `{project}.gold.daily_sales_summary`
        PARTITION BY sale_date
        CLUSTER BY product_category
        AS
        SELECT
          DATE(order_timestamp) as sale_date,
          product_category,
          COUNT(DISTINCT order_id) as order_count,
          SUM(amount) as total_revenue,
          AVG(amount) as avg_order_value
        FROM `{project}.silver.orders`
        WHERE order_status = 'completed'
        GROUP BY 1, 2
        """.format(project=project_id)
        
        logger.info("Executing query to create gold table...")
        job = client.query(query)
        job.result()  # Wait for job to complete
        
        # Check for errors
        if job.errors:
            raise Exception(f"Query failed: {job.errors}")
        
        logger.info("Successfully created gold tables")
        
        # Log table info
        table = client.get_table(f"{project_id}.gold.daily_sales_summary")
        logger.info(f"Table created: {table.num_rows} rows, {table.num_bytes} bytes")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    create_gold_tables()
```

### 7.2. Tạo Dockerfile cho Cloud Run Jobs

**Dockerfile**:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY scripts/ ./scripts/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "scripts/main.py"]
```

**requirements.txt**:

```txt
google-cloud-storage==2.14.0
google-cloud-bigquery==3.13.0
pandas==2.1.4
pyarrow==14.0.2
```

### 7.3. Deploy Cloud Run Jobs

```bash
# Build và push Docker image
gcloud builds submit --tag gcr.io/my-project/lakehouse-pipeline

# Create Cloud Run Job cho Bronze ingestion
gcloud run jobs create ingest-bronze-job \
  --image=gcr.io/my-project/lakehouse-pipeline \
  --region=us-central1 \
  --set-env-vars="GCP_PROJECT=my-project,SOURCE_BUCKET=source-bucket,BRONZE_BUCKET=my-lakehouse-bronze" \
  --service-account=data-pipeline-sa@my-project.iam.gserviceaccount.com \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --command="python" \
  --args="scripts/ingest_bronze.py"

# Create Cloud Run Job cho Silver transformation
gcloud run jobs create transform-silver-job \
  --image=gcr.io/my-project/lakehouse-pipeline \
  --region=us-central1 \
  --set-env-vars="GCP_PROJECT=my-project,BRONZE_BUCKET=my-lakehouse-bronze,SILVER_BUCKET=my-lakehouse-silver" \
  --service-account=data-pipeline-sa@my-project.iam.gserviceaccount.com \
  --memory=4Gi \
  --cpu=4 \
  --max-retries=3 \
  --task-timeout=7200 \
  --command="python" \
  --args="scripts/transform_silver.py"

# Create Cloud Run Job cho Gold aggregation
gcloud run jobs create create-gold-job \
  --image=gcr.io/my-project/lakehouse-pipeline \
  --region=us-central1 \
  --set-env-vars="GCP_PROJECT=my-project" \
  --service-account=data-pipeline-sa@my-project.iam.gserviceaccount.com \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --command="python" \
  --args="scripts/create_gold.py"
```

### 7.4. Setup Cloud Scheduler cho Automation

```bash
# Schedule Bronze ingestion (chạy hàng ngày lúc 1:00 AM)
gcloud scheduler jobs create http ingest-bronze-schedule \
  --location=us-central1 \
  --schedule="0 1 * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/my-project/jobs/ingest-bronze-job:run" \
  --http-method=POST \
  --oauth-service-account-email=scheduler-sa@my-project.iam.gserviceaccount.com \
  --time-zone="Asia/Ho_Chi_Minh"

# Schedule Silver transformation (chạy sau Bronze 1 giờ)
gcloud scheduler jobs create http transform-silver-schedule \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/my-project/jobs/transform-silver-job:run" \
  --http-method=POST \
  --oauth-service-account-email=scheduler-sa@my-project.iam.gserviceaccount.com \
  --time-zone="Asia/Ho_Chi_Minh"

# Schedule Gold aggregation (chạy sau Silver 1 giờ)
gcloud scheduler jobs create http create-gold-schedule \
  --location=us-central1 \
  --schedule="0 3 * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/my-project/jobs/create-gold-job:run" \
  --http-method=POST \
  --oauth-service-account-email=scheduler-sa@my-project.iam.gserviceaccount.com \
  --time-zone="Asia/Ho_Chi_Minh"
```

### 7.5. Main Orchestration Script (Optional)

Nếu cần chạy pipeline tuần tự trong một job:

**scripts/main.py**:

```python
"""Main orchestration script"""
import os
import sys
import logging
from scripts.ingest_bronze import ingest_to_bronze
from scripts.transform_silver import transform_to_silver
from scripts.create_gold import create_gold_tables

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run complete pipeline"""
    stage = os.getenv('PIPELINE_STAGE', 'all').lower()
    
    try:
        logger.info(f"Starting pipeline execution for stage: {stage}")
        
        if stage in ['bronze', 'all']:
            logger.info("Running Bronze ingestion...")
            ingest_to_bronze()
        
        if stage in ['silver', 'all']:
            logger.info("Running Silver transformation...")
            transform_to_silver()
        
        if stage in ['gold', 'all']:
            logger.info("Running Gold aggregation...")
            create_gold_tables()
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
```

**Chạy pipeline với stage cụ thể**:

```bash
# Chạy chỉ Bronze
gcloud run jobs execute ingest-bronze-job \
  --region=us-central1 \
  --wait

# Chạy full pipeline (nếu dùng main.py)
gcloud run jobs execute lakehouse-pipeline-job \
  --region=us-central1 \
  --update-env-vars="PIPELINE_STAGE=all" \
  --wait
```

### 7.6. Monitor Jobs

```bash
# Xem logs của job
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=ingest-bronze-job" \
  --limit=50 \
  --format=json

# Xem execution history
gcloud run jobs executions list \
  --job=ingest-bronze-job \
  --region=us-central1
```

## Bước 8: Security và Access Control

### 8.1. Setup IAM

```bash
# Create service account cho data pipeline
gcloud iam service-accounts create data-pipeline-sa \
  --display-name="Data Pipeline Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:data-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:data-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### 8.2. Column-level Security

```sql
-- Create policy tag
CREATE POLICY TAG policy_tag_pii
OPTIONS (
  allowed_values = ['pii']
);

-- Attach to column
ALTER TABLE `my-project.silver.customers`
ALTER COLUMN email
SET OPTIONS (
  policy_tags = ['projects/my-project/locations/us/taxonomies/1/policyTags/policy_tag_pii']
);
```

## Bước 9: Monitoring và Alerting

### 9.1. Setup Monitoring

```yaml
# monitoring.yaml
monitors:
  - name: "data_quality_monitor"
    type: "data_quality"
    metric: "quality_score"
    threshold: 0.95
    alert_channels:
      - email
      - slack

  - name: "pipeline_failure_monitor"
    type: "pipeline_status"
    metric: "failure_rate"
    threshold: 0.05
```

### 9.2. Create Alerts

```bash
# Create alerting policy
gcloud monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Data Quality Alert" \
  --condition-threshold-value=0.95
```

## Bước 10: Documentation

### 10.1. Data Catalog

- Register all tables trong Dataplex
- Add business descriptions
- Document data lineage
- Create data dictionaries

### 10.2. Runbooks

- Document pipeline procedures
- Create troubleshooting guides
- Document incident response

## Testing Checklist

- [ ] Test data ingestion
- [ ] Validate data quality
- [ ] Test transformation logic
- [ ] Verify partitioning
- [ ] Test query performance
- [ ] Validate security controls
- [ ] Test error handling
- [ ] Load testing
- [ ] Disaster recovery testing

## Go-Live Steps

1. Final validation
2. Backup current systems
3. Deploy pipelines
4. Monitor initial runs
5. Validate data
6. Enable alerts
7. Document issues
8. Train users

## Tài liệu tham khảo

- [BigQuery Quick Start](https://cloud.google.com/bigquery/docs/quickstarts)
- [Dataflow Templates](https://cloud.google.com/dataflow/docs/templates/overview)
- [Dataplex Setup Guide](https://cloud.google.com/dataplex/docs/getting-started)
- [Cloud Run Jobs Documentation](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Python Client Libraries](https://cloud.google.com/python/docs/reference)

