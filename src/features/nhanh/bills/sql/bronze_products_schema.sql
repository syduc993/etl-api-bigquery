-- Schema definition cho Bronze layer - Bill Products
-- External Table pointing đến GCS Parquet files

CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.nhanh_bill_products_raw`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sync-nhanhvn-project-bronze/nhanh/bill_products/**/*.parquet']
);
