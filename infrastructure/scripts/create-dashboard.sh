#!/bin/bash
# Tạo Monitoring Dashboard cho ETL pipeline

set -e

PROJECT_ID="sync-nhanhvn-project"

echo "Creating monitoring dashboard..."

# Tạo dashboard JSON
cat > /tmp/etl-dashboard.json <<EOF
{
  "displayName": "ETL Pipeline Dashboard",
  "mosaicLayout": {
    "columns": 12,
    "tiles": [
      {
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Job Execution Status",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "resource.type=\"cloud_run_job\" AND resource.labels.job_name=~\"nhanh-etl.*\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "xPos": 6,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Records Processed",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "jsonPayload.records_count",
                    "aggregation": {
                      "alignmentPeriod": "300s",
                      "perSeriesAligner": "ALIGN_SUM"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "yPos": 4,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Pipeline Latency",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "jsonPayload.duration_seconds",
                    "aggregation": {
                      "alignmentPeriod": "300s",
                      "perSeriesAligner": "ALIGN_MEAN"
                    }
                  }
                }
              }
            ]
          }
        }
      },
      {
        "xPos": 6,
        "yPos": 4,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Data Quality Score",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "jsonPayload.quality_score",
                    "aggregation": {
                      "alignmentPeriod": "300s",
                      "perSeriesAligner": "ALIGN_MEAN"
                    }
                  }
                }
              }
            ]
          }
        }
      }
    ]
  }
}
EOF

# Tạo dashboard
gcloud monitoring dashboards create \
  --config-from-file=/tmp/etl-dashboard.json \
  --project=${PROJECT_ID} \
  2>/dev/null || echo "Dashboard already exists, skipping..."

echo "Dashboard created successfully!"
echo "View dashboard at: https://console.cloud.google.com/monitoring/dashboards?project=${PROJECT_ID}"

