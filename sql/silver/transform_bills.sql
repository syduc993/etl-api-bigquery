-- Transformation từ Bronze sang Silver cho Bills
-- File này chứa SQL để clean và transform data từ bronze.bills_raw
-- sang silver.bills với các bước:
-- 1. Type casting
-- 2. Deduplication
-- 3. Flatten nested JSON (products array)
-- 4. Data validation

CREATE OR REPLACE TABLE `sync-nhanhvn-project.silver.bills`
PARTITION BY DATE(created_at)
CLUSTER BY customer_id, depot_id
AS
WITH raw_bills AS (
  SELECT
    *,
    -- Extract metadata từ file path nếu có
    _FILE_NAME as source_file
  FROM `sync-nhanhvn-project.bronze.bills_raw`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id 
      ORDER BY created.createdAt DESC
    ) as rn
  FROM raw_bills
),
cleaned AS (
  SELECT
    -- Primary fields
    CAST(id AS INT64) as bill_id,
    CAST(orderId AS INT64) as order_id,
    CAST(depotId AS INT64) as depot_id,
    
    -- Dates
    PARSE_DATE('%Y-%m-%d', date) as bill_date,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created.createdAt) as created_at,
    
    -- Type and mode
    CAST(type AS INT64) as bill_type,  -- 1 = nhập kho, 2 = xuất kho
    CAST(mode AS INT64) as bill_mode,  -- 2 = bán lẻ, 6 = bán buôn, etc.
    
    -- Customer info
    CAST(customer.id AS INT64) as customer_id,
    CAST(customer.name AS STRING) as customer_name,
    CAST(customer.mobile AS STRING) as customer_mobile,
    CAST(customer.address AS STRING) as customer_address,
    
    -- Payment info
    CAST(payment.amount AS FLOAT64) as total_amount,
    CAST(payment.discount AS FLOAT64) as discount_amount,
    CAST(payment.points AS FLOAT64) as points_earned,
    CAST(payment.usedPoints.points AS FLOAT64) as points_used,
    
    -- Created by
    CAST(created.id AS INT64) as created_by_user_id,
    CAST(created.name AS STRING) as created_by_user_name,
    
    -- Metadata
    source_file,
    CURRENT_TIMESTAMP() as processed_at
    
  FROM deduplicated
  WHERE rn = 1  -- Chỉ lấy record mới nhất cho mỗi bill_id
    AND id IS NOT NULL  -- Validate required fields
    AND date IS NOT NULL
    AND created.createdAt IS NOT NULL
)
SELECT * FROM cleaned;

-- Tạo bảng bill_products để flatten products array
CREATE OR REPLACE TABLE `sync-nhanhvn-project.silver.bill_products`
PARTITION BY DATE(created_at)
CLUSTER BY bill_id, product_id
AS
WITH raw_bills AS (
  SELECT
    CAST(id AS INT64) as bill_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created.createdAt) as created_at,
    products
  FROM `sync-nhanhvn-project.bronze.bills_raw`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND id IS NOT NULL
    AND products IS NOT NULL
)
SELECT
  bill_id,
  created_at,
  CAST(p.imexId AS INT64) as imex_id,
  CAST(p.id AS INT64) as product_id,
  CAST(p.code AS STRING) as product_code,
  CAST(p.name AS STRING) as product_name,
  CAST(p.quantity AS FLOAT64) as quantity,
  CAST(p.price AS FLOAT64) as unit_price,
  CAST(p.discount AS FLOAT64) as discount_amount,
  CAST(p.amount AS FLOAT64) as line_amount,
  CAST(p.vat.percent AS INT64) as vat_percent,
  CAST(p.vat.amount AS FLOAT64) as vat_amount,
  CURRENT_TIMESTAMP() as processed_at
FROM raw_bills,
UNNEST(products) as p
WHERE p.id IS NOT NULL
  AND p.quantity > 0  -- Validate quantity
  AND p.price >= 0;    -- Validate price

