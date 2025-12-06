# Cloud Run Jobs cho Data Pipeline

## Giới thiệu

Cloud Run Jobs là serverless solution để chạy các containerized jobs trên Google Cloud, phù hợp cho việc chạy Python scripts trong data pipeline.

## Ưu điểm

- **Serverless**: Không cần quản lý infrastructure
- **Cost-effective**: Chỉ trả tiền khi chạy
- **Scalable**: Tự động scale
- **Simple**: Dễ deploy và maintain
- **Container-based**: Flexible với dependencies

## Workflow cơ bản

```
Python Script → Docker Image → Cloud Run Job → Cloud Scheduler
```

## Setup cơ bản

### 1. Tạo Python Script

**example_script.py**:

```python
"""Example data processing script"""
import os
import sys
from google.cloud import storage, bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    project_id = os.getenv('GCP_PROJECT')
    source_bucket = os.getenv('SOURCE_BUCKET')
    
    logger.info(f"Processing data from {source_bucket}")
    
    # Your processing logic here
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(source_bucket)
    
    # Process files
    blobs = bucket.list_blobs()
    for blob in blobs:
        logger.info(f"Processing {blob.name}")
        # Process blob
    
    logger.info("Processing completed successfully")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
```

### 2. Tạo Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY *.py ./
COPY scripts/ ./scripts/

# Set environment
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "main.py"]
```

### 3. Build và Push Image

```bash
# Build image
gcloud builds submit --tag gcr.io/my-project/data-pipeline:latest

# Hoặc dùng Docker
docker build -t gcr.io/my-project/data-pipeline:latest .
docker push gcr.io/my-project/data-pipeline:latest
```

### 4. Tạo Cloud Run Job

```bash
gcloud run jobs create my-data-job \
  --image=gcr.io/my-project/data-pipeline:latest \
  --region=us-central1 \
  --set-env-vars="GCP_PROJECT=my-project,SOURCE_BUCKET=my-bucket" \
  --service-account=my-sa@my-project.iam.gserviceaccount.com \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --task-timeout=3600
```

### 5. Execute Job

```bash
# Manual execution
gcloud run jobs execute my-data-job \
  --region=us-central1 \
  --wait

# View logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=my-data-job" \
  --limit=50
```

## Scheduling với Cloud Scheduler

### Tạo Schedule

```bash
# Enable Cloud Run Admin API
gcloud services enable run.googleapis.com

# Grant Scheduler permission
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:scheduler-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# Create schedule (chạy hàng ngày lúc 1:00 AM)
gcloud scheduler jobs create http my-data-job-schedule \
  --location=us-central1 \
  --schedule="0 1 * * *" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/my-project/jobs/my-data-job:run" \
  --http-method=POST \
  --oauth-service-account-email=scheduler-sa@my-project.iam.gserviceaccount.com \
  --time-zone="Asia/Ho_Chi_Minh"
```

### Schedule Format

- `0 1 * * *` - Hàng ngày lúc 1:00 AM
- `0 */6 * * *` - Mỗi 6 giờ
- `0 0 * * 1` - Mỗi thứ 2 lúc 0:00
- `*/30 * * * *` - Mỗi 30 phút

## Best Practices

### 1. Error Handling

```python
import sys
import logging

try:
    # Your code
    pass
except Exception as e:
    logging.error(f"Error: {str(e)}", exc_info=True)
    sys.exit(1)  # Exit với code != 0 để job được mark là failed
```

### 2. Logging

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Starting processing...")
logger.error("Error occurred...")
```

### 3. Environment Variables

```python
import os

# Required variables
project_id = os.getenv('GCP_PROJECT')
if not project_id:
    raise ValueError("GCP_PROJECT environment variable is required")

# Optional với defaults
batch_size = int(os.getenv('BATCH_SIZE', '1000'))
```

### 4. Resource Sizing

- **Memory**: Estimate dựa trên data size
- **CPU**: Thường cần ít hơn memory
- **Timeout**: Đủ thời gian để job hoàn thành

```bash
# Ví dụ sizing
--memory=4Gi      # Cho large data processing
--cpu=4           # Parallel processing
--task-timeout=7200  # 2 hours
```

### 5. Retry Strategy

```bash
--max-retries=3        # Số lần retry
--task-timeout=3600    # Timeout per task
```

### 6. Service Account

Tạo service account riêng cho jobs:

```bash
# Create service account
gcloud iam service-accounts create data-pipeline-sa \
  --display-name="Data Pipeline Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:data-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:data-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

## Dependency Management

### Sequential Jobs

Nếu job B phụ thuộc vào job A:

**Option 1: Sử dụng Cloud Scheduler delay**

```bash
# Job A chạy lúc 1:00 AM
gcloud scheduler jobs create http job-a-schedule \
  --schedule="0 1 * * *" \
  ...

# Job B chạy sau Job A 1 giờ
gcloud scheduler jobs create http job-b-schedule \
  --schedule="0 2 * * *" \
  ...
```

**Option 2: Trigger từ script**

```python
# Trong job A script
from google.cloud import run_v2

def trigger_job_b():
    client = run_v2.JobsClient()
    request = run_v2.RunJobRequest(
        name="projects/my-project/locations/us-central1/jobs/job-b"
    )
    client.run_job(request=request)
```

**Option 3: Pub/Sub trigger**

```python
# Job A publishes message khi xong
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, 'job-b-trigger')
publisher.publish(topic_path, b'Job A completed')
```

## Monitoring

### View Job Status

```bash
# List executions
gcloud run jobs executions list \
  --job=my-data-job \
  --region=us-central1

# Get execution details
gcloud run jobs executions describe EXECUTION_NAME \
  --region=us-central1
```

### View Logs

```bash
# Real-time logs
gcloud logging tail "resource.type=cloud_run_job AND resource.labels.job_name=my-data-job"

# Filter by severity
gcloud logging read "resource.type=cloud_run_job AND severity>=ERROR" \
  --limit=50
```

### Setup Alerts

```bash
# Create alert policy cho job failures
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Cloud Run Job Failure Alert" \
  --condition-threshold-value=1 \
  --condition-filter='resource.type="cloud_run_job" AND metric.type="run.googleapis.com/job/execution_count" AND metric.label.status="failed"'
```

## Cost Optimization

### 1. Right-sizing Resources

- Monitor actual usage
- Adjust memory/CPU based on metrics
- Use smallest resources that work

### 2. Reduce Execution Time

- Optimize code
- Parallel processing
- Incremental processing

### 3. Smart Scheduling

- Schedule during off-peak hours
- Batch multiple tasks
- Avoid unnecessary runs

## Example: Complete Pipeline

### Project Structure

```
project/
├── Dockerfile
├── requirements.txt
├── main.py
└── scripts/
    ├── ingest_bronze.py
    ├── transform_silver.py
    └── create_gold.py
```

### Deployment Script

```bash
#!/bin/bash
# deploy.sh

PROJECT_ID="my-project"
REGION="us-central1"
IMAGE_NAME="lakehouse-pipeline"

# Build và push image
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${IMAGE_NAME}:latest

# Deploy jobs
gcloud run jobs create ingest-bronze-job \
  --image=gcr.io/${PROJECT_ID}/${IMAGE_NAME}:latest \
  --region=${REGION} \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
  --memory=2Gi \
  --cpu=2 \
  --command="python" \
  --args="scripts/ingest_bronze.py"

# Create schedules
gcloud scheduler jobs create http ingest-bronze-schedule \
  --location=${REGION} \
  --schedule="0 1 * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/ingest-bronze-job:run" \
  --http-method=POST \
  --oauth-service-account-email=scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com
```

## Troubleshooting

### Job không chạy

```bash
# Check job configuration
gcloud run jobs describe my-data-job --region=us-central1

# Check service account permissions
gcloud projects get-iam-policy my-project \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:my-sa@my-project.iam.gserviceaccount.com"
```

### Job failed

```bash
# Xem logs chi tiết
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=my-data-job AND severity>=ERROR" \
  --limit=100

# Check execution status
gcloud run jobs executions describe EXECUTION_NAME --region=us-central1
```

### Performance issues

- Increase memory/CPU
- Optimize code
- Use parallel processing
- Check network latency

## So sánh với các giải pháp khác

| Feature | Cloud Run Jobs | Cloud Composer | Dataproc |
|---------|----------------|----------------|----------|
| Setup complexity | Low | High | Medium |
| Cost | Pay per execution | Always-on | Pay per cluster |
| Maintenance | Minimal | High | Medium |
| Suitable for | Simple scripts | Complex workflows | Heavy processing |

## Tài liệu tham khảo

- [Cloud Run Jobs Documentation](https://cloud.google.com/run/docs/create-jobs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
- [Python Client Libraries](https://cloud.google.com/python/docs/reference)
- [Best Practices](https://cloud.google.com/run/docs/tips)

