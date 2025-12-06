#!/bin/bash
# Script to set up Cloud Scheduler for daily ETL job execution

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="asia-southeast1"
JOB_NAME="nhanh-etl-job"
SCHEDULER_NAME="daily-etl-job"
TIMEZONE="Asia/Ho_Chi_Minh"
SCHEDULE="0 2 * * *"  # 2 AM daily (Vietnam time)

echo "Setting up Cloud Scheduler for daily ETL execution..."

# Check if scheduler already exists
if gcloud scheduler jobs describe $SCHEDULER_NAME --location=$REGION --project=$PROJECT_ID 2>/dev/null; then
  echo "Scheduler already exists, updating..."
  gcloud scheduler jobs update http $SCHEDULER_NAME \
    --location=$REGION \
    --schedule="$SCHEDULE" \
    --time-zone="$TIMEZONE" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
    --message-body='{"overrides":{"containerOverrides":[{"args":["--platform=nhanh","--entity=all"]}]}}'
else
  echo "Creating new scheduler..."
  gcloud scheduler jobs create http $SCHEDULER_NAME \
    --location=$REGION \
    --schedule="$SCHEDULE" \
    --time-zone="$TIMEZONE" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
    --message-body='{"overrides":{"containerOverrides":[{"args":["--platform=nhanh","--entity=all"]}]}}' \
    --description="Daily ETL job to extract data from Nhanh API"
fi

echo ""
echo "✅ Cloud Scheduler setup completed!"
echo ""
echo "Scheduler details:"
echo "  Name: $SCHEDULER_NAME"
echo "  Schedule: $SCHEDULE (2 AM daily, Vietnam time)"
echo "  Timezone: $TIMEZONE"
echo "  Target Job: $JOB_NAME"
echo ""
echo "To test the scheduler manually:"
echo "  gcloud scheduler jobs run $SCHEDULER_NAME --location=$REGION"
echo ""
echo "To view scheduler:"
echo "  gcloud scheduler jobs list --location=$REGION"

