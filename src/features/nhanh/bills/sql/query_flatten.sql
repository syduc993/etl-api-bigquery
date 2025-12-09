-- Query to flatten and insert bills data
-- Source: External Table (Parquet) -> Target: Native Table (Flattened)
-- Updated to read from Parquet format with STRUCT columns

MERGE `{project_id}.{dataset}.fact_sales_bills_v3_0` T
USING (
    SELECT
        id,
        depotId,
        PARSE_DATE('%Y-%m-%d', date) AS date,
        type,
        mode,
        
        -- Customer Flattening (from STRUCT)
        customer.id AS customer_id,
        customer.name AS customer_name,
        customer.mobile AS customer_mobile,
        customer.address AS customer_address,
        
        -- Sale Flattening (from STRUCT)
        sale.id AS sale_id,
        sale.name AS sale_name,
        created.id AS created_id,
        created.name AS created_email,
        
        -- Payment Flattening (from STRUCT)
        payment.amount AS payment_total_amount,
        payment.customerAmount AS payment_customer_amount,
        payment.discount AS payment_discount,
        payment.points AS payment_points,
        
        -- Nested Payment Methods (from STRUCT)
        payment.cash.amount AS payment_cash_amount,
        payment.transfer.amount AS payment_transfer_amount,
        payment.transfer.accountId AS payment_transfer_account_id,
        payment.credit.amount AS payment_credit_amount,
        
        description,
        CURRENT_TIMESTAMP() AS extraction_timestamp
        
    FROM `{project_id}.{dataset_raw}.nhanh_bills_raw` -- Parquet External table
) S
ON T.id = S.id
WHEN MATCHED THEN
    UPDATE SET
        depotId = S.depotId,
        date = S.date,
        type = S.type,
        mode = S.mode,
        customer_id = S.customer_id,
        customer_name = S.customer_name,
        customer_mobile = S.customer_mobile,
        customer_address = S.customer_address,
        payment_total_amount = S.payment_total_amount,
        payment_customer_amount = S.payment_customer_amount,
        payment_discount = S.payment_discount,
        payment_points = S.payment_points,
        payment_cash_amount = S.payment_cash_amount,
        payment_transfer_amount = S.payment_transfer_amount,
        payment_transfer_account_id = S.payment_transfer_account_id,
        payment_credit_amount = S.payment_credit_amount,
        description = S.description,
        extraction_timestamp = S.extraction_timestamp
WHEN NOT MATCHED THEN
    INSERT ROW;

-- Insert Bill Products from separate bill_products_raw table (Parquet)
INSERT INTO `{project_id}.{dataset}.fact_sales_bills_product_v3_0`
SELECT 
    bill_id,
    id AS product_id,
    code AS product_code,
    barcode AS product_barcode,
    name AS product_name,
    quantity,
    price,
    discount,
    vat.percent AS vat_percent,
    vat.amount AS vat_amount,
    amount,
    CURRENT_TIMESTAMP() AS extraction_timestamp
FROM `{project_id}.{dataset_raw}.nhanh_bill_products_raw`; -- Parquet External table
