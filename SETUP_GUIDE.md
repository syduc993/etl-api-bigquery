# Setup Guide

Hướng dẫn setup dự án ETL từ Nhanh.vn API đến Google Cloud Lakehouse.

## Bước 1: Setup Secrets trong Secret Manager

Chạy script để tạo secrets:

```bash
cd infrastructure/scripts
chmod +x setup-secrets.sh
./setup-secrets.sh
```

Hoặc tạo thủ công:

```bash
# Tạo secrets
echo -n "YOUR_APP_ID" | gcloud secrets create nhanh-app-id --data-file=-
echo -n "YOUR_BUSINESS_ID" | gcloud secrets create nhanh-business-id --data-file=-
echo -n "YOUR_ACCESS_TOKEN" | gcloud secrets create nhanh-access-token --data-file=-

# Grant permissions
PROJECT_ID="sync-nhanhvn-project"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

gcloud secrets add-iam-policy-binding nhanh-app-id \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding nhanh-business-id \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding nhanh-access-token \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor"
```

## Bước 2: Setup Service Account Permissions

Đảm bảo service account có đủ permissions:

```bash
PROJECT_ID="sync-nhanhvn-project"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

# BigQuery permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/bigquery.jobUser"

# Storage permissions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/storage.objectAdmin"

# Secret Manager permissions (already done in step 1)
```

## Bước 3: Deploy Cloud Run Job

```bash
cd infrastructure/scripts
chmod +x deploy.sh
./deploy.sh
```

Hoặc deploy thủ công:

```bash
PROJECT_ID="sync-nhanhvn-project"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/nhanh-etl"
JOB_NAME="nhanh-etl-job"

# Build image
gcloud builds submit --tag ${IMAGE_NAME}

# Create job
gcloud run jobs create ${JOB_NAME} \
  --image=${IMAGE_NAME} \
  --region=${REGION} \
  --service-account=${PROJECT_ID}@appspot.gserviceaccount.com \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCP_REGION=${REGION},BRONZE_BUCKET=${PROJECT_ID}-bronze,SILVER_BUCKET=${PROJECT_ID}-silver,BRONZE_DATASET=bronze,SILVER_DATASET=silver,GOLD_DATASET=gold,PARTITION_STRATEGY=month,LOG_LEVEL=INFO" \
  --set-secrets="NHANH_APP_ID=nhanh-app-id:latest,NHANH_BUSINESS_ID=nhanh-business-id:latest,NHANH_ACCESS_TOKEN=nhanh-access-token:latest"
```

## Bước 4: Test Job

```bash
# Execute job manually
gcloud run jobs execute nhanh-etl-job --region=us-central1 --wait

# Check logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=nhanh-etl-job" --limit=50
```

## Bước 5: Setup Cloud Scheduler (Optional)

Để tự động chạy job mỗi 30 phút:

```bash
cd infrastructure/scripts
chmod +x setup-scheduler.sh
./setup-scheduler.sh
```

## Bước 6: Verify Data

Kiểm tra data đã được upload:

```bash
# List files in Bronze bucket
gsutil ls -r gs://sync-nhanhvn-project-bronze/

# Check watermark table
bq query --use_legacy_sql=false \
  "SELECT * FROM \`sync-nhanhvn-project.bronze.extraction_watermarks\`"
```

## Troubleshooting

### Lỗi Authentication

- Kiểm tra secrets đã được tạo và có permissions
- Verify service account có quyền truy cập secrets

### Lỗi Rate Limit

- Job tự động handle rate limit với exponential backoff
- Có thể giảm frequency của scheduler nếu cần

### Lỗi BigQuery

- Kiểm tra datasets đã được tạo
- Verify service account có BigQuery permissions

## Next Steps

Sau khi Bronze layer hoạt động, tiếp tục với:
- Phase 3: Silver Layer transformations (SQL scripts)
- Phase 4: Gold Layer aggregations
- Phase 5: Monitoring và alerting

Xem `etl-plan/PROJECT_PLAN.md` để biết chi tiết.

