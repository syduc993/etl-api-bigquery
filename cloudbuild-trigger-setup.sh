#!/bin/bash
# Script to set up Cloud Build trigger for automatic deployment

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="us-central1"
REPO_NAME="etl-api-bigquery"
REPO_OWNER="syduc993"
TRIGGER_NAME="deploy-to-cloud-run"

echo "Setting up Cloud Build trigger for automatic deployment..."

# Connect GitHub repository to Cloud Build (if not already connected)
echo "Checking GitHub connection..."
gcloud source repos list --format="value(name)" | grep -q "^${REPO_NAME}$" || {
  echo "Connecting GitHub repository..."
  gcloud builds triggers create github \
    --name="${TRIGGER_NAME}" \
    --repo-name="${REPO_NAME}" \
    --repo-owner="${REPO_OWNER}" \
    --branch-pattern="^main$" \
    --build-config="cloudbuild.yaml" \
    --region="${REGION}" \
    --project="${PROJECT_ID}"
}

echo "Cloud Build trigger setup completed!"
echo "The trigger will automatically deploy to Cloud Run when you push to main branch."

