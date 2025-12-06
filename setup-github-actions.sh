#!/bin/bash
# Script to set up GitHub Actions service account and key

set -e

PROJECT_ID="sync-nhanhvn-project"
SA_NAME="github-actions"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Setting up GitHub Actions service account..."

# Check if service account exists
if gcloud iam service-accounts describe $SA_EMAIL --project=$PROJECT_ID 2>/dev/null; then
  echo "Service account already exists: $SA_EMAIL"
else
  echo "Creating service account..."
  gcloud iam service-accounts create $SA_NAME \
    --display-name="GitHub Actions Service Account" \
    --project=$PROJECT_ID
fi

# Grant necessary permissions
echo "Granting permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.admin" \
  --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.admin" \
  --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/iam.serviceAccountUser" \
  --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/artifactregistry.writer" \
  --condition=None

# Create key
KEY_FILE="github-actions-key.json"
if [ -f "$KEY_FILE" ]; then
  echo "Key file already exists: $KEY_FILE"
  read -p "Do you want to create a new key? (y/N) " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f $KEY_FILE
  else
    echo "Using existing key file."
    exit 0
  fi
fi

echo "Creating service account key..."
gcloud iam service-accounts keys create $KEY_FILE \
  --iam-account=$SA_EMAIL \
  --project=$PROJECT_ID

echo ""
echo "✅ Setup completed!"
echo ""
echo "Next steps:"
echo "1. Copy the contents of $KEY_FILE"
echo "2. Go to: https://github.com/syduc993/etl-api-bigquery/settings/secrets/actions"
echo "3. Click 'New repository secret'"
echo "4. Name: GCP_SA_KEY"
echo "5. Value: Paste the entire JSON content from $KEY_FILE"
echo "6. Click 'Add secret'"
echo ""
echo "⚠️  Keep $KEY_FILE secure and do not commit it to git!"

