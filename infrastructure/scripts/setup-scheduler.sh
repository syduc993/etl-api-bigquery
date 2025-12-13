#!/bin/bash
# Setup Cloud Scheduler cho ETL pipeline

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="us-central1"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

echo "Setting up Cloud Scheduler jobs..."

# Schedule Bronze extraction mỗi 30 phút
echo "Creating scheduler for Bronze extraction (every 30 minutes)..."
gcloud scheduler jobs create http nhanh-etl-bronze-schedule \
  --location=${REGION} \
  --schedule="*/30 * * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/nhanh-etl-job:run" \
  --http-method=POST \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --time-zone="Asia/Ho_Chi_Minh" \
  --project=${PROJECT_ID} \
  --message-body='{"args":["--entity","all","--incremental"]}' \
  2>/dev/null || echo "Bronze scheduler already exists, skipping..."

# Schedule Silver transformation sau Bronze 15 phút
echo "Creating scheduler for Silver transformation (every 45 minutes, offset 15 mins)..."
gcloud scheduler jobs create http nhanh-etl-silver-schedule \
  --location=${REGION} \
  --schedule="15,45 * * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/nhanh-etl-silver-job:run" \
  --http-method=POST \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --time-zone="Asia/Ho_Chi_Minh" \
  --project=${PROJECT_ID} \
  --message-body='{"args":["--entity","all"]}' \
  2>/dev/null || echo "Silver scheduler already exists, skipping..."

# Schedule Gold aggregation sau Silver 15 phút
echo "Creating scheduler for Gold aggregation (every hour, offset 30 mins)..."
gcloud scheduler jobs create http nhanh-etl-gold-schedule \
  --location=${REGION} \
  --schedule="30 * * * *" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/nhanh-etl-gold-job:run" \
  --http-method=POST \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --time-zone="Asia/Ho_Chi_Minh" \
  --project=${PROJECT_ID} \
  --message-body='{"args":["--aggregate","all"]}' \
  2>/dev/null || echo "Gold scheduler already exists, skipping..."

# Schedule Nhanh Bills Daily Sync (chạy hàng ngày lúc 1:00 AM)
echo "Creating scheduler for Nhanh Bills daily sync (daily at 1:00 AM)..."
gcloud scheduler jobs create http etl-api-bigquery-nhanh-bills-daily-sync-schedule \
  --location=${REGION} \
  --schedule="0 1 * * *" \
  --uri="https://asia-southeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/etl-api-bigquery-nhanh-bills-daily-sync:run" \
  --http-method=POST \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --time-zone="Asia/Ho_Chi_Minh" \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Nhanh Bills daily sync scheduler already exists, skipping..."

# Schedule OneOffice Daily Sync (chạy hàng ngày lúc 1:00 AM)
echo "Creating scheduler for OneOffice daily sync (daily at 1:00 AM)..."
gcloud scheduler jobs create http etl-api-bigquery-oneoffice-daily-sync-schedule \
  --location=${REGION} \
  --schedule="0 1 * * *" \
  --uri="https://asia-southeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/etl-api-bigquery-oneoffice-daily-sync:run" \
  --http-method=POST \
  --oauth-service-account-email=${SERVICE_ACCOUNT} \
  --time-zone="Asia/Ho_Chi_Minh" \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "OneOffice scheduler already exists, skipping..."

echo "Scheduler setup completed!"
echo ""
echo "Schedules:"
echo "  - Bronze: Every 30 minutes (0, 30)"
echo "  - Silver: Every 45 minutes, offset 15 mins (15, 45)"
echo "  - Gold: Every hour, offset 30 mins (30)"
echo "  - Nhanh Bills Daily Sync: Daily at 1:00 AM"
echo "  - OneOffice Daily Sync: Daily at 1:00 AM"
