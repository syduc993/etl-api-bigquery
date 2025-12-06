#!/bin/bash
# Chạy Silver layer transformation

set -e

PROJECT_ID="sync-nhanhvn-project"
ENTITY=${1:-all}

echo "Running Silver transformation for entity: ${ENTITY}"

# Chạy transformation SQL
if [ "${ENTITY}" == "all" ] || [ "${ENTITY}" == "bills" ]; then
    echo "Transforming bills..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/silver/transform_bills.sql
fi

if [ "${ENTITY}" == "all" ] || [ "${ENTITY}" == "products" ]; then
    echo "Transforming products..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/silver/transform_products.sql
fi

if [ "${ENTITY}" == "all" ] || [ "${ENTITY}" == "customers" ]; then
    echo "Transforming customers..."
    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} < sql/silver/transform_customers.sql
fi

echo "Silver transformation completed!"

