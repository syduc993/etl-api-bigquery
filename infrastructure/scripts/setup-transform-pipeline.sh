#!/bin/bash
# Setup đầy đủ Transform Pipeline: Build image → Create Job → Setup Trigger & Scheduler
# Script này sẽ setup tất cả components cần thiết cho transform pipeline

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="asia-southeast1"
BUCKET_NAME="${PROJECT_ID}"  # Bronze bucket
JOB_NAME="nhanh-bills-transform-job"
SCHEDULER_NAME="nhanh-bills-transform-schedule"
TRIGGER_NAME="gcs-trigger-bills-transform"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
IMAGE_NAME="gcr.io/${PROJECT_ID}/nhanh-etl:latest"

echo "🚀 Setting up Transform Pipeline..."
echo "   Project: ${PROJECT_ID}"
echo "   Region: ${REGION}"
echo "   Job: ${JOB_NAME}"
echo ""

# Step 1: Build Docker image
echo "📦 Step 1: Building Docker image..."
cd "$(dirname "$0")/../.."  # Go to project root
gcloud builds submit --tag ${IMAGE_NAME} --project=${PROJECT_ID} || {
    echo "❌ Failed to build image. Make sure you're in the project root directory."
    exit 1
}
echo "✅ Image built: ${IMAGE_NAME}"
echo ""

# Step 2: Create Cloud Run Job
echo "🚀 Step 2: Creating Cloud Run Job..."
if gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} 2>/dev/null; then
    echo "   Job exists, updating..."
    gcloud run jobs update ${JOB_NAME} \
        --image=${IMAGE_NAME} \
        --region=${REGION} \
        --service-account=${SERVICE_ACCOUNT} \
        --memory=2Gi \
        --cpu=2 \
        --max-retries=2 \
        --task-timeout=1800 \
        --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCP_REGION=${REGION},BRONZE_BUCKET=${BUCKET_NAME},BRONZE_DATASET=bronze,TARGET_DATASET=nhanhVN,PARTITION_STRATEGY=month,LOG_LEVEL=INFO" \
        --command="python" \
        --args="src/features/nhanh/bills/transform_trigger.py" \
        --project=${PROJECT_ID}
else
    echo "   Creating new job..."
    gcloud run jobs create ${JOB_NAME} \
        --image=${IMAGE_NAME} \
        --region=${REGION} \
        --service-account=${SERVICE_ACCOUNT} \
        --memory=2Gi \
        --cpu=2 \
        --max-retries=2 \
        --task-timeout=1800 \
        --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCP_REGION=${REGION},BRONZE_BUCKET=${BUCKET_NAME},BRONZE_DATASET=bronze,TARGET_DATASET=nhanhVN,PARTITION_STRATEGY=month,LOG_LEVEL=INFO" \
        --command="python" \
        --args="src/features/nhanh/bills/transform_trigger.py" \
        --project=${PROJECT_ID}
fi
echo "✅ Job created/updated: ${JOB_NAME}"
echo ""

# Step 3: Enable required APIs for Eventarc
echo "📦 Step 3: Enabling required APIs..."
gcloud services enable \
    eventarc.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    logging.googleapis.com \
    storage-api.googleapis.com \
    --project=${PROJECT_ID} \
    --quiet
echo "✅ APIs enabled"
echo ""

# Step 4: Grant Eventarc permissions
echo "🔐 Step 4: Granting Eventarc permissions..."
EVENTARC_SA="service-$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')@gcp-sa-eventarc.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/eventarc.eventReceiver" \
    --quiet 2>/dev/null || echo "   Permission already granted"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/storage.objectViewer" \
    --quiet 2>/dev/null || echo "   Permission already granted"
echo "✅ Permissions granted"
echo ""

# Step 5: Create Eventarc trigger
echo "📝 Step 5: Creating Eventarc trigger..."
if gcloud eventarc triggers describe ${TRIGGER_NAME} --location=${REGION} --project=${PROJECT_ID} 2>/dev/null; then
    echo "   Trigger exists, deleting old one..."
    gcloud eventarc triggers delete ${TRIGGER_NAME} \
        --location=${REGION} \
        --project=${PROJECT_ID} \
        --quiet
fi

gcloud eventarc triggers create ${TRIGGER_NAME} \
    --location=${REGION} \
    --destination-run-job=${JOB_NAME} \
    --destination-run-job-region=${REGION} \
    --destination-run-job-task-timeout=1800 \
    --event-filters="type=google.cloud.storage.object.v1.finalized" \
    --event-filters="bucket=${BUCKET_NAME}" \
    --event-filters="objectNamePrefix=nhanh/" \
    --event-filters="objectNameSuffix=.parquet" \
    --service-account=${SERVICE_ACCOUNT} \
    --project=${PROJECT_ID}
echo "✅ Trigger created: ${TRIGGER_NAME}"
echo ""

# Step 6: Setup Cloud Scheduler (if not exists)
echo "⏰ Step 6: Checking Cloud Scheduler..."
JOB_API_URL="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe ${SCHEDULER_NAME} --location=${REGION} --project=${PROJECT_ID} 2>/dev/null; then
    echo "   Scheduler exists, updating..."
    gcloud scheduler jobs update http ${SCHEDULER_NAME} \
        --location=${REGION} \
        --schedule="0 * * * *" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
        --http-method=POST \
        --oauth-service-account-email=${SERVICE_ACCOUNT} \
        --time-zone="Asia/Ho_Chi_Minh" \
        --project=${PROJECT_ID}
else
    echo "   Creating scheduler..."
    gcloud scheduler jobs create http ${SCHEDULER_NAME} \
        --location=${REGION} \
        --schedule="0 * * * *" \
        --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
        --http-method=POST \
        --oauth-service-account-email=${SERVICE_ACCOUNT} \
        --time-zone="Asia/Ho_Chi_Minh" \
        --project=${PROJECT_ID}
fi
echo "✅ Scheduler setup: ${SCHEDULER_NAME}"
echo ""

echo "✅✅✅ SETUP COMPLETED! ✅✅✅"
echo ""
echo "📋 Summary:"
echo "   ✅ Docker Image: ${IMAGE_NAME}"
echo "   ✅ Cloud Run Job: ${JOB_NAME}"
echo "   ✅ Eventarc Trigger: ${TRIGGER_NAME}"
echo "   ✅ Cloud Scheduler: ${SCHEDULER_NAME} (runs every hour)"
echo ""
echo "🧪 To test:"
echo "   1. Upload a file to gs://${BUCKET_NAME}/nhanh/bills/ or nhanh/bill_products/"
echo "   2. Check job executions:"
echo "      gcloud run jobs executions list --job=${JOB_NAME} --region=${REGION} --limit=5"
echo "   3. Or manually trigger:"
echo "      gcloud run jobs execute ${JOB_NAME} --region=${REGION}"
echo ""
echo "📊 To view logs:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50"

