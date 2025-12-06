@echo off
REM Script to run ETL pipeline locally with date range

set PYTHONPATH=%CD%
set GCP_PROJECT=sync-nhanhvn-project
set GCP_REGION=asia-southeast1
set BRONZE_BUCKET=sync-nhanhvn-project-bronze
set SILVER_BUCKET=sync-nhanhvn-project-silver
set BRONZE_DATASET=bronze
set SILVER_DATASET=silver
set GOLD_DATASET=gold
set PARTITION_STRATEGY=month
set LOG_LEVEL=INFO

echo Running ETL pipeline locally...
echo Date Range: 2025-12-01 to 2025-12-05
echo.

python src/main.py --platform=nhanh --entity=all --from-date=2025-12-01 --to-date=2025-12-05

pause

