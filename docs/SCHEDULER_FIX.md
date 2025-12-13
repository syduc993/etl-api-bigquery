# Fix Cloud Scheduler Issue

## Vấn đề

Cloud Scheduler đã setup nhưng **KHÔNG CHẠY ĐƯỢC** vì:
- ❌ Cloud Run Job `nhanh-bills-transform-job` **KHÔNG TỒN TẠI**
- ❌ Scheduler fail với status code: 5 (NOT_FOUND)

## Đã fix

✅ **Data 29/11 đã được sync** bằng cách chạy transform thủ công:
- Bills: 9,266 rows ✅
- Products: 19,489 rows ✅

✅ **Fixed transformer path** - SQL files path đã được sửa

## Giải pháp để Scheduler hoạt động

### Option 1: Tạo Cloud Run Job (Khuyến nghị)

**Bước 1: Build Docker image** (nếu chưa có):
```bash
gcloud builds submit --tag gcr.io/sync-nhanhvn-project/nhanh-etl:latest
```

**Bước 2: Tạo Cloud Run Job**:
```bash
python src/scripts/create_cloud_run_job.py
```

Hoặc thủ công:
```bash
gcloud run jobs create nhanh-bills-transform-job \
  --image=gcr.io/sync-nhanhvn-project/nhanh-etl:latest \
  --region=asia-southeast1 \
  --service-account=sync-nhanhvn-project@appspot.gserviceaccount.com \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=2 \
  --task-timeout=1800 \
  --set-env-vars="GCP_PROJECT=sync-nhanhvn-project,GCP_REGION=asia-southeast1,BRONZE_BUCKET=sync-nhanhvn-project,BRONZE_DATASET=bronze,TARGET_DATASET=nhanhVN" \
  --command="python" \
  --args="src/features/nhanh/bills/transform_trigger.py"
```

**Bước 3: Verify Scheduler**:
```bash
# Check scheduler status
gcloud scheduler jobs describe nhanh-bills-transform-schedule --location asia-southeast1

# Manually trigger to test
gcloud scheduler jobs run nhanh-bills-transform-schedule --location asia-southeast1
```

### Option 2: Chạy thủ công mỗi giờ

Nếu không muốn setup Cloud Run Job, có thể:
- Chạy script Python trực tiếp: `python src/scripts/run_transform.py`
- Setup cron job trên server local

## Kiểm tra Scheduler

```bash
# Check last execution
gcloud scheduler jobs describe nhanh-bills-transform-schedule --location asia-southeast1

# Check execution history
gcloud scheduler jobs list --location asia-southeast1

# View logs
gcloud logging read "resource.type=cloud_scheduler_job AND resource.labels.job_id=nhanh-bills-transform-schedule" --limit 10
```

## Lưu ý

- Transform SQL **KHÔNG có filter date** - nó sẽ process **TẤT CẢ** data từ external tables
- MERGE logic đảm bảo không duplicate khi re-run
- Cloud Scheduler sẽ tự động chạy mỗi 1 giờ sau khi job được tạo

