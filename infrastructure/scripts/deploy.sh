#!/bin/bash
# Deploy script for Cloud Run Jobs

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="us-central1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/nhanh-etl"
JOB_NAME="nhanh-etl-job"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

echo "Building Docker image..."
gcloud builds submit --tag ${IMAGE_NAME}

echo "Creating/updating Cloud Run Job..."
gcloud run jobs create ${JOB_NAME} \
  --image=${IMAGE_NAME} \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT} \
  --memory=2Gi \
  --cpu=2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCP_REGION=${REGION},BRONZE_BUCKET=${PROJECT_ID}-bronze,SILVER_BUCKET=${PROJECT_ID}-silver,BRONZE_DATASET=bronze,SILVER_DATASET=silver,GOLD_DATASET=gold,PARTITION_STRATEGY=month,LOG_LEVEL=INFO" \
  --set-secrets="NHANH_APP_ID=nhanh-app-id:latest,NHANH_BUSINESS_ID=nhanh-business-id:latest,NHANH_ACCESS_TOKEN=nhanh-access-token:latest" \
  || gcloud run jobs update ${JOB_NAME} \
    --region=${REGION} \
    --image=${IMAGE_NAME} \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCP_REGION=${REGION},BRONZE_BUCKET=${PROJECT_ID}-bronze,SILVER_BUCKET=${PROJECT_ID}-silver,BRONZE_DATASET=bronze,SILVER_DATASET=silver,GOLD_DATASET=gold,PARTITION_STRATEGY=month,LOG_LEVEL=INFO" \
    --set-secrets="NHANH_APP_ID=nhanh-app-id:latest,NHANH_BUSINESS_ID=nhanh-business-id:latest,NHANH_ACCESS_TOKEN=nhanh-access-token:latest"

echo "Deployment completed!"
echo "To execute the job, run:"
echo "  gcloud run jobs execute ${JOB_NAME} --region=${REGION}"

