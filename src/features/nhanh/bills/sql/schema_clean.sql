-- Schema definition for simplified/flattened bills tables
-- Dataset: nhanhVN, Region: Singapore (asia-southeast1)

-- Table: fact_sales_bills_v3.0 (Dữ liệu hóa đơn phẳng)
CREATE OR REPLACE TABLE `{project_id}.{dataset}.fact_sales_bills_v3_0` (
    id INT64,
    depotId INT64,
    date DATE,
    type INT64,
    mode INT64,
    
    -- Customer info (Flattened from "customer" object)
    customer_id INT64,
    customer_name STRING,
    customer_mobile STRING,
    customer_address STRING,
    
    -- Sale/Staff info
    sale_id INT64,
    sale_name STRING,
    created_id INT64,
    created_email STRING,
    
    -- Payment info (Flattened from "payment" object)
    payment_total_amount FLOAT64, -- payment.amount
    payment_customer_amount FLOAT64,
    payment_discount FLOAT64,
    payment_points FLOAT64,
    
    -- Flattened payment methods
    payment_cash_amount FLOAT64,
    payment_transfer_amount FLOAT64,
    payment_transfer_account_id INT64,
    payment_credit_amount FLOAT64,
    
    description STRING,
    extraction_timestamp TIMESTAMP
)
PARTITION BY date
CLUSTER BY depotId, type;

-- Table: fact_sales_bills_product_v3.0 (Chi tiết sản phẩm trong hóa đơn)
CREATE OR REPLACE TABLE `{project_id}.{dataset}.fact_sales_bills_product_v3_0` (
    bill_id INT64,
    
    -- Product info
    product_id INT64, -- products[].id
    product_code STRING,
    product_barcode STRING,
    product_name STRING,
    
    -- Transaction info
    quantity FLOAT64,
    price FLOAT64,
    discount FLOAT64,
    vat_percent INT64,
    vat_amount FLOAT64,
    amount FLOAT64, -- Thành tiền
    
    -- Metadata
    bill_date DATE, -- Date from bills table for partitioning
    extraction_timestamp TIMESTAMP
)
PARTITION BY bill_date
CLUSTER BY bill_id, product_id;
