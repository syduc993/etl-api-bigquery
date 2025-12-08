#!/bin/bash
# Setup BigQuery External Tables cho Bronze layer

set -e

PROJECT_ID="sync-nhanhvn-project"

echo "Setting up BigQuery External Tables for Bronze layer..."

# Create External Table cho Bills (Nhanh)
echo "Creating external table for nhanh/bills..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.nhanh_bills_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/nhanh/bills/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

# Create External Table cho Bill Products (Nhanh - products đã tách từ bills)
echo "Creating external table for nhanh/bill_products..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.nhanh_bill_products_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/nhanh/bill_products/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

# Create External Table cho Products (Nhanh - standalone products entity)
echo "Creating external table for nhanh/products..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.nhanh_products_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/nhanh/products/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

# Create External Table cho Customers (Nhanh)
echo "Creating external table for nhanh/customers..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE EXTERNAL TABLE \`${PROJECT_ID}.bronze.nhanh_customers_raw\`
OPTIONS (
  format = 'JSON',
  uris = ['gs://${PROJECT_ID}-bronze/nhanh/customers/**/*.json.gz'],
  compression = 'GZIP'
);
EOF

echo "Bronze layer external tables created successfully!"

