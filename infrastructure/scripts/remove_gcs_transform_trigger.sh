#!/bin/bash
# Remove Eventarc trigger for GCS transform

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="asia-southeast1"
TRIGGER_NAME="gcs-trigger-bills-transform"

echo "🗑️  Removing Eventarc trigger: ${TRIGGER_NAME}..."

gcloud eventarc triggers delete ${TRIGGER_NAME} \
    --location=${REGION} \
    --project=${PROJECT_ID} \
    --quiet

echo "✅ Trigger removed!"

