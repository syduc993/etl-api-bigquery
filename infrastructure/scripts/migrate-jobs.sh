#!/bin/bash
# Migrate old Cloud Run Jobs to new naming convention
# This script creates new jobs with new names and updates schedulers

set -e

PROJECT_ID="sync-nhanhvn-project"
REGION="asia-southeast1"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

echo "=========================================="
echo "Migrating Cloud Run Jobs to new naming convention"
echo "=========================================="
echo ""

# Migration mapping: old_name -> new_name
declare -A MIGRATIONS=(
    ["nhanh-bills-transform-job"]="etl-api-bigquery-nhanh-bills-transform"
    ["oneoffice-daily-sync-job"]="etl-api-bigquery-oneoffice-daily-sync"
)

# Scheduler migration mapping: old_scheduler -> new_scheduler -> new_job
declare -A SCHEDULER_MIGRATIONS=(
    ["oneoffice-daily-sync-schedule"]="etl-api-bigquery-oneoffice-daily-sync-schedule,etl-api-bigquery-oneoffice-daily-sync"
)

echo "Note: This script will:"
echo "  1. Create new jobs with new names (if they don't exist)"
echo "  2. Update schedulers to point to new jobs"
echo "  3. Keep old jobs for backward compatibility (you can delete them later)"
echo ""
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled"
    exit 1
fi

# Get current image
CURRENT_IMAGE=$(gcloud run jobs describe nhanh-bills-transform-job --region=$REGION --project=$PROJECT_ID --format="value(spec.template.spec.containers[0].image)" 2>/dev/null || echo "gcr.io/${PROJECT_ID}/nhanh-etl:latest")

echo ""
echo "Using image: $CURRENT_IMAGE"
echo ""

# Migrate jobs
for old_name in "${!MIGRATIONS[@]}"; do
    new_name="${MIGRATIONS[$old_name]}"
    
    echo "Migrating: $old_name -> $new_name"
    
    # Check if old job exists
    if ! gcloud run jobs describe "$old_name" --region=$REGION --project=$PROJECT_ID 2>/dev/null; then
        echo "  ⚠️  Old job $old_name not found, skipping..."
        continue
    fi
    
    # Get old job config
    OLD_CONFIG=$(gcloud run jobs describe "$old_name" --region=$REGION --project=$PROJECT_ID --format=json)
    
    # Check if new job already exists
    if gcloud run jobs describe "$new_name" --region=$REGION --project=$PROJECT_ID 2>/dev/null; then
        echo "  ✅ New job $new_name already exists, skipping creation..."
    else
        echo "  Creating new job: $new_name"
        
        # Extract config from old job
        MEMORY=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].resources.limits.memory // "2Gi"')
        CPU=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].resources.limits.cpu // "2"')
        MAX_RETRIES=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.maxRetries // 2')
        TIMEOUT=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.timeoutSeconds // 1800')
        COMMAND=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].command[0] // empty')
        ARGS=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].args[0] // empty')
        
        # Build env vars
        ENV_VARS=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].env[] | "\(.name)=\(.value)"' | tr '\n' ',' | sed 's/,$//')
        
        # Build secrets
        SECRETS=$(echo "$OLD_CONFIG" | jq -r '.spec.template.spec.containers[0].env[] | select(.valueFrom.secretKeyRef) | "\(.name)=\(.valueFrom.secretKeyRef.name):latest"' | tr '\n' ',' | sed 's/,$//')
        
        # Create new job with labels
        CREATE_CMD="gcloud run jobs create $new_name \
          --image=$CURRENT_IMAGE \
          --region=$REGION \
          --service-account=$SERVICE_ACCOUNT \
          --memory=$MEMORY \
          --cpu=$CPU \
          --max-retries=$MAX_RETRIES \
          --task-timeout=$TIMEOUT \
          --labels=project=etl-api-bigquery,feature=$(echo $new_name | cut -d'-' -f4-5),type=$(echo $new_name | cut -d'-' -f6),environment=production \
          --project=$PROJECT_ID"
        
        if [ -n "$ENV_VARS" ]; then
            CREATE_CMD="$CREATE_CMD --set-env-vars=$ENV_VARS"
        fi
        
        if [ -n "$SECRETS" ]; then
            CREATE_CMD="$CREATE_CMD --set-secrets=$SECRETS"
        fi
        
        if [ -n "$COMMAND" ] && [ "$COMMAND" != "null" ]; then
            CREATE_CMD="$CREATE_CMD --command=$COMMAND"
        fi
        
        if [ -n "$ARGS" ] && [ "$ARGS" != "null" ]; then
            CREATE_CMD="$CREATE_CMD --args=$ARGS"
        fi
        
        eval $CREATE_CMD
        echo "  ✅ New job created: $new_name"
    fi
done

echo ""
echo "=========================================="
echo "Migration completed!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Update schedulers using: bash infrastructure/scripts/setup-scheduler.sh"
echo "  2. Test new jobs manually"
echo "  3. Delete old jobs after verification:"
for old_name in "${!MIGRATIONS[@]}"; do
    echo "     gcloud run jobs delete $old_name --region=$REGION --project=$PROJECT_ID"
done

