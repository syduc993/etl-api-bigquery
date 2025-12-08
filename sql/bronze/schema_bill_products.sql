-- Schema definition cho Bronze layer - Bill Products
-- File này định nghĩa schema cho BigQuery External Table
-- pointing đến GCS JSON files của bill_products (products đã tách ra từ bills)

CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.nhanh_bill_products_raw`
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/nhanh/bill_products/**/*.json.gz'],
  compression = 'GZIP'
);

-- Schema sẽ được auto-detect từ JSON files
-- Có thể thêm explicit schema nếu cần:
/*
CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.bill_products_raw`
(
  bill_id STRING,
  product_id STRING,
  imexId INT64,
  id INT64,
  code STRING,
  name STRING,
  quantity FLOAT64,
  price FLOAT64,
  discount FLOAT64,
  amount FLOAT64,
  vat STRUCT<
    percent INT64,
    amount FLOAT64
  >
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/nhanh/bill_products/**/*.json.gz'],
  compression = 'GZIP'
);
*/

