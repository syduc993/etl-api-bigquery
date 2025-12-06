#!/bin/bash
# Setup BigQuery External Tables cho Bronze layer

set -e

PROJECT_ID="sync-nhanhvn-project"

echo "Setting up BigQuery External Tables for Bronze layer..."

# Create External Table cho Bills
echo "Creating external table for bills..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.bills_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/bills/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

# Create External Table cho Products
echo "Creating external table for products..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.products_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/products/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

# Create External Table cho Customers
echo "Creating external table for customers..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.customers_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/customers/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

echo "Bronze layer external tables created successfully!"

