# Deployment Guide

## Automatic Deployment Setup

Code đã được push lên GitHub: https://github.com/syduc993/etl-api-bigquery.git

### Cloud Build Trigger Setup

Để thiết lập tự động deploy khi push code lên GitHub:

1. **Qua Cloud Console:**
   - Vào [Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers?project=sync-nhanhvn-project)
   - Click "Create Trigger"
   - Chọn source: GitHub (syduc993/etl-api-bigquery)
   - Branch: `^main$`
   - Configuration: Cloud Build configuration file
   - Location: `cloudbuild.yaml`
   - Region: `us-central1`
   - Click "Create"

2. **Qua gcloud CLI:**
   ```bash
   gcloud builds triggers create github \
     --name="deploy-etl-to-cloud-run" \
     --repo-name="etl-api-bigquery" \
     --repo-owner="syduc993" \
     --branch-pattern="^main$" \
     --build-config="cloudbuild.yaml" \
     --region="us-central1"
   ```

### Manual Deployment

Nếu cần deploy thủ công:

```bash
gcloud builds submit --config cloudbuild.yaml
```

### Running Jobs with Date Range

Để chạy job với date range cụ thể:

```bash
gcloud run jobs execute nhanh-etl-job \
  --region=us-central1 \
  --args="--platform=nhanh,--entity=all,--from-date=2025-12-01,--to-date=2025-12-05"
```

Hoặc cho một entity cụ thể:

```bash
gcloud run jobs execute nhanh-etl-job \
  --region=us-central1 \
  --args="--platform=nhanh,--entity=bills,--from-date=2025-12-01,--to-date=2025-12-05"
```

### Cloud Run Job Configuration

- **Job Name**: `nhanh-etl-job`
- **Region**: `us-central1`
- **Memory**: 2Gi
- **CPU**: 2
- **Max Retries**: 3
- **Timeout**: 3600s (1 hour)

### Environment Variables

- `GCP_PROJECT`: sync-nhanhvn-project
- `GCP_REGION`: us-central1
- `BRONZE_BUCKET`: sync-nhanhvn-project-bronze
- `SILVER_BUCKET`: sync-nhanhvn-project-silver
- `BRONZE_DATASET`: bronze
- `SILVER_DATASET`: silver
- `GOLD_DATASET`: gold
- `PARTITION_STRATEGY`: month
- `LOG_LEVEL`: INFO

### Secrets

- `NHANH_APP_ID`: nhanh-app-id:latest
- `NHANH_BUSINESS_ID`: nhanh-business-id:latest
- `NHANH_ACCESS_TOKEN`: nhanh-access-token:latest

