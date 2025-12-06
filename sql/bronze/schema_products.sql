-- Schema definition cho Bronze layer - Products
-- File này định nghĩa schema cho BigQuery External Table
-- pointing đến GCS JSON files của products

CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.products_raw`
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/products/**/*.json.gz'],
  compression = 'GZIP'
);

-- Schema sẽ được auto-detect từ JSON files

