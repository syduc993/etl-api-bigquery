#!/bin/bash
# Setup Eventarc trigger để tự động chạy transform khi có file mới trong GCS
# Khi có file .parquet mới upload vào nhanh/bills/ hoặc nhanh/bill_products/,
# Eventarc sẽ trigger Cloud Run Job để chạy transform

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="asia-southeast1"
BUCKET_NAME="${PROJECT_ID}"  # Bronze bucket (sync-nhanhvn-project)
JOB_NAME="nhanh-bills-transform-job"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
IMAGE_NAME="gcr.io/${PROJECT_ID}/nhanh-etl:latest"

echo "🔧 Setting up GCS Eventarc trigger for automatic transform..."
echo "   Project: ${PROJECT_ID}"
echo "   Bucket: ${BUCKET_NAME}"
echo "   Region: ${REGION}"
echo ""

# Step 1: Enable required APIs
echo "📦 Enabling required APIs..."
gcloud services enable \
    eventarc.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    logging.googleapis.com \
    storage-api.googleapis.com \
    --project=${PROJECT_ID} \
    --quiet

# Step 2: Grant Eventarc service account permissions
echo "🔐 Granting Eventarc permissions..."
EVENTARC_SA="service-$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')@gcp-sa-eventarc.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/eventarc.eventReceiver" \
    --quiet 2>/dev/null || echo "   Permission already granted"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${EVENTARC_SA}" \
    --role="roles/storage.objectViewer" \
    --quiet 2>/dev/null || echo "   Permission already granted"

# Step 3: Create Cloud Run Job for transform (nếu chưa có)
echo "🚀 Checking Cloud Run Job: ${JOB_NAME}..."
if ! gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} 2>/dev/null; then
    echo "   Creating Cloud Run Job for transform..."
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
    echo "   ✅ Created job: ${JOB_NAME}"
else
    echo "   ✅ Job already exists: ${JOB_NAME}"
fi

# Step 4: Create single Eventarc trigger (triggers on both bills and bill_products)
# Using single trigger with prefix to avoid duplicate transforms
echo "📝 Creating Eventarc trigger for nhanh/ data..."
TRIGGER_NAME="gcs-trigger-bills-transform"

# Delete existing trigger if exists
if gcloud eventarc triggers describe ${TRIGGER_NAME} --location=${REGION} --project=${PROJECT_ID} 2>/dev/null; then
    echo "   Deleting existing trigger..."
    gcloud eventarc triggers delete ${TRIGGER_NAME} \
        --location=${REGION} \
        --project=${PROJECT_ID} \
        --quiet
fi

# Create new trigger (triggers on any .parquet file in nhanh/ prefix)
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

echo "   ✅ Created trigger: ${TRIGGER_NAME}"

echo ""
echo "✅ Setup completed!"
echo ""
echo "📋 Summary:"
echo "   - Trigger: ${TRIGGER_NAME}"
echo "   - Triggers on: gs://${BUCKET_NAME}/nhanh/**/*.parquet"
echo "   - Job: ${JOB_NAME}"
echo "   - Action: Automatically runs transform when new Parquet files are uploaded"
echo ""
echo "🧪 To test:"
echo "   1. Upload a new file to gs://${BUCKET_NAME}/nhanh/bills/ or nhanh/bill_products/"
echo "   2. Check Cloud Run Job executions:"
echo "      gcloud run jobs executions list --job=${JOB_NAME} --region=${REGION} --limit=5"
echo ""
echo "📊 To view trigger:"
echo "   gcloud eventarc triggers describe ${TRIGGER_NAME} --location=${REGION}"

