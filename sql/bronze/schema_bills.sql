-- Schema definition cho Bronze layer - Bills
-- File này định nghĩa schema cho BigQuery External Table
-- pointing đến GCS JSON files của bills

CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.nhanh_bills_raw`
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/nhanh/bills/**/*.json.gz'],
  compression = 'GZIP'
);

-- Schema sẽ được auto-detect từ JSON files
-- Có thể thêm explicit schema nếu cần:
/*
CREATE OR REPLACE EXTERNAL TABLE `sync-nhanhvn-project.bronze.bills_raw`
(
  id INT64,
  orderId INT64,
  depotId INT64,
  date DATE,
  type INT64,
  mode INT64,
  customer STRUCT<
    id INT64,
    name STRING,
    mobile STRING,
    address STRING
  >,
  products ARRAY<STRUCT<
    imexId INT64,
    id INT64,
    code STRING,
    name STRING,
    quantity FLOAT64,
    price FLOAT64,
    amount FLOAT64
  >>,
  payment STRUCT<
    amount FLOAT64,
    discount FLOAT64
  >,
  created STRUCT<
    id INT64,
    name STRING,
    createdAt TIMESTAMP
  >
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://sync-nhanhvn-project-bronze/bills/**/*.json.gz'],
  compression = 'GZIP'
);
*/

