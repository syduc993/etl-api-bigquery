"""
Setup Cloud Scheduler để chạy transform tự động mỗi 1 tiếng.
"""
import subprocess
import sys
import os

PROJECT_ID = "sync-nhanhvn-project"
REGION = "asia-southeast1"
JOB_NAME = "nhanh-bills-transform-job"
SCHEDULER_NAME = "nhanh-bills-transform-schedule"
SERVICE_ACCOUNT = f"{PROJECT_ID}@appspot.gserviceaccount.com"

# Schedule: Mỗi 1 tiếng (cron: 0 * * * *)
SCHEDULE = "0 * * * *"


def run_command(cmd, description):
    """Run a shell command and return success status."""
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"{'='*60}")
    cmd_str = cmd if isinstance(cmd, str) else ' '.join(cmd)
    print(f"Command: {cmd_str}")
    print()
    
    try:
        result = subprocess.run(
            cmd_str,
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
    print("🔧 Setting up Cloud Scheduler for automatic transform...")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Region: {REGION}")
    print(f"   Schedule: Mỗi 1 tiếng ({SCHEDULE})")
    print(f"   Job: {JOB_NAME}")
    
    # Build the API endpoint URL for Cloud Run Job
    job_api_url = f"https://{REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/{PROJECT_ID}/jobs/{JOB_NAME}:run"
    
    # Check if scheduler already exists
    print(f"\n🔍 Checking if scheduler already exists...")
    result = subprocess.run(
        f'gcloud scheduler jobs describe {SCHEDULER_NAME} --location {REGION} --project {PROJECT_ID}',
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"   ⚠️  Scheduler exists, deleting...")
        if not run_command(
            f'gcloud scheduler jobs delete {SCHEDULER_NAME} --location {REGION} --project {PROJECT_ID} --quiet',
            "Deleting existing scheduler"
        ):
            print("   ⚠️  Failed to delete, will try to update instead")
    
    # Create/Update Cloud Scheduler
    if run_command(
        f'gcloud scheduler jobs create http {SCHEDULER_NAME} --location {REGION} --schedule "{SCHEDULE}" --uri "{job_api_url}" --http-method POST --oauth-service-account-email {SERVICE_ACCOUNT} --time-zone "Asia/Ho_Chi_Minh" --project {PROJECT_ID}',
        f"Creating Cloud Scheduler: {SCHEDULER_NAME}"
    ):
        print(f"\n{'='*60}")
        print(f"✅ SETUP COMPLETED!")
        print(f"{'='*60}")
        print(f"\n📋 Scheduler Details:")
        print(f"   Name: {SCHEDULER_NAME}")
        print(f"   Region: {REGION}")
        print(f"   Schedule: Mỗi 1 tiếng (cron: {SCHEDULE})")
        print(f"   Target: Cloud Run Job '{JOB_NAME}'")
        print(f"\n🧪 To test manually:")
        print(f"   gcloud scheduler jobs run {SCHEDULER_NAME} --location {REGION}")
        print(f"\n📊 To view scheduler:")
        print(f"   gcloud scheduler jobs describe {SCHEDULER_NAME} --location {REGION}")
        return 0
    else:
        print(f"\n❌ Setup failed. Please check errors above.")
        return 1


if __name__ == "__main__":
    exit(main())

