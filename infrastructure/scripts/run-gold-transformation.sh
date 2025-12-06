#!/bin/bash
# Chạy Gold layer transformation

set -e

PROJECT_ID="sync-nhanhvn-project"
AGGREGATE=${1:-all}

echo "Running Gold transformation for aggregate: ${AGGREGATE}"

# Chạy transformation SQL
if [ "${AGGREGATE}" == "all" ] || [ "${AGGREGATE}" == "daily_revenue" ]; then
    echo "Creating daily revenue summary..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/gold/daily_revenue_summary.sql
fi

if [ "${AGGREGATE}" == "all" ] || [ "${AGGREGATE}" == "customer_clv" ]; then
    echo "Creating customer lifetime value..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/gold/customer_lifetime_value.sql
fi

if [ "${AGGREGATE}" == "all" ] || [ "${AGGREGATE}" == "product_metrics" ]; then
    echo "Creating product sales metrics..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/gold/product_sales_metrics.sql
fi

if [ "${AGGREGATE}" == "all" ] || [ "${AGGREGATE}" == "inventory" ]; then
    echo "Creating inventory analytics..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/gold/inventory_analytics.sql
fi

echo "Gold transformation completed!"

