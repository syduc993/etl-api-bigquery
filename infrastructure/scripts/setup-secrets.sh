#!/bin/bash
# Setup secrets in Secret Manager

set -e

PROJECT_ID="sync-nhanhvn-project"

echo "Setting up secrets in Secret Manager..."
echo ""
echo "Please provide the following values:"

read -p "Nhanh App ID: " APP_ID
read -p "Nhanh Business ID: " BUSINESS_ID
read -sp "Nhanh Access Token: " ACCESS_TOKEN
echo ""

# Create secrets
echo "Creating secrets..."

echo -n "${APP_ID}" | gcloud secrets create nhanh-app-id \
  --data-file=- \
  --project=${PROJECT_ID} \
  2>/dev/null || echo -n "${APP_ID}" | gcloud secrets versions add nhanh-app-id \
  --data-file=- \
  --project=${PROJECT_ID}

echo -n "${BUSINESS_ID}" | gcloud secrets create nhanh-business-id \
  --data-file=- \
  --project=${PROJECT_ID} \
  2>/dev/null || echo -n "${BUSINESS_ID}" | gcloud secrets versions add nhanh-business-id \
  --data-file=- \
  --project=${PROJECT_ID}

echo -n "${ACCESS_TOKEN}" | gcloud secrets create nhanh-access-token \
  --data-file=- \
  --project=${PROJECT_ID} \
  2>/dev/null || echo -n "${ACCESS_TOKEN}" | gcloud secrets versions add nhanh-access-token \
  --data-file=- \
  --project=${PROJECT_ID}

echo ""
echo "Secrets created successfully!"
echo ""
echo "Granting access to service account..."
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

gcloud secrets add-iam-policy-binding nhanh-app-id \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor" \
  --project=${PROJECT_ID}

gcloud secrets add-iam-policy-binding nhanh-business-id \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor" \
  --project=${PROJECT_ID}

gcloud secrets add-iam-policy-binding nhanh-access-token \
  --member="serviceAccount:${SERVICE_ACCOUNT}" \
  --role="roles/secretmanager.secretAccessor" \
  --project=${PROJECT_ID}

echo "Setup completed!"

