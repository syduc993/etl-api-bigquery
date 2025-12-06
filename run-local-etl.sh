#!/bin/bash
# Script to run ETL pipeline locally with date range

export PYTHONPATH=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/gcp-mcp/service-account-key.json
export GCP_PROJECT=sync-nhanhvn-project
export GCP_REGION=asia-southeast1
export BRONZE_BUCKET=sync-nhanhvn-project-bronze
export SILVER_BUCKET=sync-nhanhvn-project-silver
export BRONZE_DATASET=bronze
export SILVER_DATASET=silver
export GOLD_DATASET=gold
export PARTITION_STRATEGY=month
export LOG_LEVEL=INFO

echo "Running ETL pipeline locally..."
echo "Date Range: 2025-12-01 to 2025-12-05"
echo "Using service account: $GOOGLE_APPLICATION_CREDENTIALS"
echo ""

python src/main.py --platform=nhanh --entity=all --from-date=2025-12-01 --to-date=2025-12-05

