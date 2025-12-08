-- Schema definition cho Bronze layer - Customers
-- File này định nghĩa schema cho BigQuery External Table
-- pointing đến GCS JSON files của customers

CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.nhanh_customers_raw`
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/nhanh/customers/**/*.json.gz'],
  compression = 'GZIP'
);

-- Schema sẽ được auto-detect từ JSON files

