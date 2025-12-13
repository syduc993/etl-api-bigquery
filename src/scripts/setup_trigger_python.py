"""
Python script to setup Eventarc trigger using gcloud CLI.
This script will setup automatic transform trigger when new Parquet files are uploaded to GCS.
"""
import subprocess
import sys
import os

PROJECT_ID = "sync-nhanhvn-project"
REGION = "asia-southeast1"
BUCKET_NAME = "sync-nhanhvn-project"
JOB_NAME = "nhanh-bills-transform-job"
SERVICE_ACCOUNT = f"{PROJECT_ID}@appspot.gserviceaccount.com"
TRIGGER_NAME = "gcs-trigger-bills-transform"


def run_command(cmd, description):
    """Run a shell command and return success status."""
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"{'='*60}")
    cmd_str = ' '.join(cmd) if isinstance(cmd, list) else cmd
    print(f"Command: {cmd_str}")
    print()
    
    try:
        # Use shell=True on Windows to find gcloud in PATH
        result = subprocess.run(
            cmd_str if isinstance(cmd, list) else cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            print(f"✅ Success!")
            if result.stdout:
                print(result.stdout)
            return True
        else:
            print(f"⚠️  Exit code: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            if result.stdout:
                print(result.stdout)
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False


def main():
    """Main setup function."""
    print("🔧 Setting up GCS Eventarc trigger for automatic transform...")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Bucket: {BUCKET_NAME}")
    print(f"   Region: {REGION}")
    
    success_count = 0
    total_steps = 0
    
    # Step 1: Enable APIs
    total_steps += 1
    if run_command(
        f'gcloud services enable eventarc.googleapis.com cloudbuild.googleapis.com run.googleapis.com logging.googleapis.com storage-api.googleapis.com --project {PROJECT_ID} --quiet',
        "Step 1: Enabling required APIs"
    ):
        success_count += 1
    
    # Step 2: Get project number and grant permissions
    total_steps += 1
    try:
        result = subprocess.run(
            f'gcloud projects describe {PROJECT_ID} --format=value(projectNumber)',
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        project_number = result.stdout.strip()
        eventarc_sa = f"service-{project_number}@gcp-sa-eventarc.iam.gserviceaccount.com"
        
        print(f"\n🔐 Granting Eventarc permissions...")
        print(f"   Eventarc SA: {eventarc_sa}")
        
        # Grant eventarc.eventReceiver
        run_command(
            f'gcloud projects add-iam-policy-binding {PROJECT_ID} --member="serviceAccount:{eventarc_sa}" --role="roles/eventarc.eventReceiver" --condition=None --quiet',
            "Granting eventarc.eventReceiver role"
        )
        
        # Grant storage.objectViewer
        if run_command(
            f'gcloud projects add-iam-policy-binding {PROJECT_ID} --member="serviceAccount:{eventarc_sa}" --role="roles/storage.objectViewer" --condition=None --quiet',
            "Granting storage.objectViewer role"
        ):
            success_count += 1
    except Exception as e:
        print(f"❌ Failed to grant permissions: {e}")
    
    # Step 3: Check if Cloud Run Job exists
    total_steps += 1
    print(f"\n🚀 Checking Cloud Run Job: {JOB_NAME}...")
    result = subprocess.run(
        f'gcloud run jobs describe {JOB_NAME} --region {REGION} --project {PROJECT_ID}',
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"   ✅ Job exists: {JOB_NAME}")
        success_count += 1
    else:
        print(f"   ⚠️  Job not found. You may need to create it first.")
        print(f"   Create with: gcloud run jobs create {JOB_NAME} --image=gcr.io/{PROJECT_ID}/nhanh-etl:latest --region={REGION}")
    
    # Step 4: Delete existing trigger if exists
    print(f"\n🗑️  Checking for existing trigger...")
    result = subprocess.run(
        f'gcloud eventarc triggers describe {TRIGGER_NAME} --location {REGION} --project {PROJECT_ID}',
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"   Trigger exists, deleting...")
        run_command(
            f'gcloud eventarc triggers delete {TRIGGER_NAME} --location {REGION} --project {PROJECT_ID} --quiet',
            f"Deleting existing trigger: {TRIGGER_NAME}"
        )
    
    # Step 5: Create Eventarc trigger
    total_steps += 1
    # For Cloud Run Jobs, use destination-run-job with full path format
    job_path = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
    if run_command(
        f'gcloud eventarc triggers create {TRIGGER_NAME} --location {REGION} --destination-run-job {job_path} --event-filters="type=google.cloud.storage.object.v1.finalized" --event-filters="bucket={BUCKET_NAME}" --event-filters="objectNamePrefix=nhanh/" --event-filters="objectNameSuffix=.parquet" --service-account {SERVICE_ACCOUNT} --project {PROJECT_ID}',
        f"Step 5: Creating Eventarc trigger: {TRIGGER_NAME}"
    ):
        success_count += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 SETUP SUMMARY")
    print(f"{'='*60}")
    print(f"   Steps completed: {success_count}/{total_steps}")
    
    if success_count == total_steps:
        print(f"\n✅ Setup completed successfully!")
        print(f"\n📋 Trigger Details:")
        print(f"   Name: {TRIGGER_NAME}")
        print(f"   Region: {REGION}")
        print(f"   Destination: Cloud Run Job '{JOB_NAME}'")
        print(f"   Filters: gs://{BUCKET_NAME}/nhanh/**/*.parquet")
        print(f"\n🧪 To test, upload a file:")
        print(f"   echo 'test' | gsutil cp - gs://{BUCKET_NAME}/nhanh/bills/test.parquet")
        return 0
    else:
        print(f"\n⚠️  Some steps failed. Please check errors above.")
        return 1


if __name__ == "__main__":
    exit(main())

