"""
Setup Cloud Scheduler to trigger the webview messcard Cloud Run Job daily at 09:05 GMT+7.
"""
import subprocess
import sys


PROJECT_ID = "sync-nhanhvn-project"
REGION = "asia-southeast1"
JOB_NAME = "webview-messenger-reader-messcard-daily"
SCHEDULER_NAME = "webview-messenger-reader-messcard-schedule"
SERVICE_ACCOUNT = f"{PROJECT_ID}@appspot.gserviceaccount.com"
CRON = "5 16 * * *"  # 16:05 GMT+7
TIMEZONE = "Asia/Ho_Chi_Minh"


def run_cmd(cmd: str) -> bool:
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.strip())
        return False
    return True


def main() -> int:
    job_api_url = (
        f"https://{REGION}-run.googleapis.com/apis/run.googleapis.com/"
        f"v1/namespaces/{PROJECT_ID}/jobs/{JOB_NAME}:run"
    )

    # Delete existing scheduler if present
    subprocess.run(
        f"gcloud scheduler jobs describe {SCHEDULER_NAME} --location {REGION} --project {PROJECT_ID}",
        shell=True,
        capture_output=True,
        text=True,
    )
    run_cmd(
        f"gcloud scheduler jobs delete {SCHEDULER_NAME} "
        f"--location {REGION} --project {PROJECT_ID} --quiet"
    )

    ok = run_cmd(
        "gcloud scheduler jobs create http "
        f"{SCHEDULER_NAME} "
        f"--location {REGION} "
        f'--schedule "{CRON}" '
        f'--uri "{job_api_url}" '
        "--http-method POST "
        f"--oauth-service-account-email {SERVICE_ACCOUNT} "
        f'--time-zone "{TIMEZONE}" '
        f"--project {PROJECT_ID}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

