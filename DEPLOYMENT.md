# Deployment Guide

## Automatic Deployment với GitHub Actions

Code đã được push lên GitHub: https://github.com/syduc993/etl-api-bigquery.git

### Thiết lập GitHub Actions (Tự động deploy khi push code)

1. **Tạo Service Account Key cho GitHub:**
   ```bash
   # Tạo service account (nếu chưa có)
   gcloud iam service-accounts create github-actions \
     --display-name="GitHub Actions Service Account" \
     --project=sync-nhanhvn-project

   # Gán quyền cần thiết
   gcloud projects add-iam-policy-binding sync-nhanhvn-project \
     --member="serviceAccount:github-actions@sync-nhanhvn-project.iam.gserviceaccount.com" \
     --role="roles/run.admin"

   gcloud projects add-iam-policy-binding sync-nhanhvn-project \
     --member="serviceAccount:github-actions@sync-nhanhvn-project.iam.gserviceaccount.com" \
     --role="roles/storage.admin"

   gcloud projects add-iam-policy-binding sync-nhanhvn-project \
     --member="serviceAccount:github-actions@sync-nhanhvn-project.iam.gserviceaccount.com" \
     --role="roles/iam.serviceAccountUser"

   # Tạo key
   gcloud iam service-accounts keys create github-actions-key.json \
     --iam-account=github-actions@sync-nhanhvn-project.iam.gserviceaccount.com
   ```

2. **Thêm Secret vào GitHub:**
   - Vào repository: https://github.com/syduc993/etl-api-bigquery
   - Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `GCP_SA_KEY`
   - Value: Copy toàn bộ nội dung file `github-actions-key.json` (JSON format)
   - Click "Add secret"

3. **Tự động deploy:**
   - Mỗi khi push code lên branch `main`, GitHub Actions sẽ tự động:
     - Build Docker image
     - Push lên Google Container Registry
     - Deploy/update Cloud Run Job

### Chạy Job với Date Range qua GitHub Actions

1. Vào repository trên GitHub
2. Click tab **Actions**
3. Chọn workflow **"Run ETL Job with Date Range"**
4. Click **"Run workflow"**
5. Nhập:
   - **from_date**: `2025-12-01`
   - **to_date**: `2025-12-05`
   - **entity**: `all` (hoặc `bills`, `products`, `customers`, `orders`)
6. Click **"Run workflow"**

### Manual Deployment (nếu cần)

Nếu cần deploy thủ công:

```bash
gcloud builds submit --config cloudbuild.yaml
```

### Running Jobs với Date Range (Qua gcloud CLI)

Nếu muốn chạy trực tiếp từ terminal:

```bash
gcloud run jobs execute nhanh-etl-job \
  --region=asia-southeast1 \
  --args="--platform=nhanh,--entity=all,--from-date=2025-12-01,--to-date=2025-12-05"
```

Hoặc cho một entity cụ thể:

```bash
gcloud run jobs execute nhanh-etl-job \
  --region=asia-southeast1 \
  --args="--platform=nhanh,--entity=bills,--from-date=2025-12-01,--to-date=2025-12-05"
```

### Cloud Run Job Configuration

- **Job Name**: `nhanh-etl-job`
- **Region**: `asia-southeast1`
- **Memory**: 2Gi
- **CPU**: 2
- **Max Retries**: 3
- **Timeout**: 3600s (1 hour)

### Environment Variables

- `GCP_PROJECT`: sync-nhanhvn-project
- `GCP_REGION`: asia-southeast1
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

