#!/bin/bash
# Load testing script cho ETL pipeline

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="us-central1"
JOB_NAME="nhanh-etl-job"

echo "Starting load test for ETL pipeline..."
echo ""

# Test 1: Concurrent job executions
echo "Test 1: Concurrent job executions (5 concurrent jobs)"
for i in {1..5}; do
  echo "Starting job execution $i..."
  gcloud run jobs execute ${JOB_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --async &
done

wait
echo "Test 1 completed"
echo ""

# Test 2: Sequential executions với high frequency
echo "Test 2: Sequential executions (10 jobs, 1 minute apart)"
for i in {1..10}; do
  echo "Starting job execution $i..."
  gcloud run jobs execute ${JOB_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --wait
  
  if [ $i -lt 10 ]; then
    echo "Waiting 60 seconds before next execution..."
    sleep 60
  fi
done

echo "Test 2 completed"
echo ""

# Test 3: Large data volume test
echo "Test 3: Large data volume test (full sync)"
gcloud run jobs execute ${JOB_NAME} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --args="--entity,all,--full-sync" \
  --wait

echo "Test 3 completed"
echo ""

# Check results
echo "Checking job execution history..."
gcloud run jobs executions list \
  --job=${JOB_NAME} \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --limit=20

echo ""
echo "Load test completed!"
echo "Review logs and metrics to analyze performance."

