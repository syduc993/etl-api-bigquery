#!/bin/bash
# Tạo alerting policies cho ETL pipeline

set -e

PROJECT_ID="sync-nhanhvn-project"

echo "Creating alerting policies..."

# Alert cho job failures
echo "Creating alert for job failures..."
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="ETL Job Failure Alert" \
  --condition-display-name="Job execution failed" \
  --condition-threshold-value=1 \
  --condition-threshold-duration=0s \
  --condition-filter='resource.type="cloud_run_job" AND resource.labels.job_name=~"nhanh-etl.*" AND jsonPayload.status="failure"' \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Job failure alert already exists, skipping..."

# Alert cho data quality below threshold
echo "Creating alert for data quality..."
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Data Quality Alert" \
  --condition-display-name="Data quality below 95%" \
  --condition-threshold-value=0.95 \
  --condition-threshold-duration=300s \
  --condition-filter='jsonPayload.quality_score < 0.95' \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Data quality alert already exists, skipping..."

# Alert cho rate limit hits
echo "Creating alert for frequent rate limit hits..."
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Rate Limit Alert" \
  --condition-display-name="Rate limit hit frequently" \
  --condition-threshold-value=5 \
  --condition-threshold-duration=300s \
  --condition-filter='jsonPayload.locked_seconds > 0' \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Rate limit alert already exists, skipping..."

# Alert cho pipeline latency spike
echo "Creating alert for pipeline latency..."
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Pipeline Latency Alert" \
  --condition-display-name="Pipeline latency > 1 hour" \
  --condition-threshold-value=3600 \
  --condition-threshold-duration=600s \
  --condition-filter='jsonPayload.duration_seconds > 3600' \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Pipeline latency alert already exists, skipping..."

echo "Alerting policies created!"
echo ""
echo "Note: You need to replace CHANNEL_ID with actual notification channel ID"
echo "Create notification channels first:"
echo "  gcloud alpha monitoring channels create --display-name='Email Alert' --type=email --channel-labels=email_address=your-email@example.com"

