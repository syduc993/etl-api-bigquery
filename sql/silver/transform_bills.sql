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
  FROM `sync-nhanhvn-project.bronze.nhanh_bills_raw`
  -- Parquet external tables don't support _PARTITIONTIME, will filter on date field after parsing
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
    PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) as bill_date,
    -- Handle created.createdAt: could be INT64 (timestamp) or STRING
    CASE 
      WHEN created.createdAt IS NULL THEN NULL
      WHEN SAFE_CAST(created.createdAt AS INT64) IS NOT NULL 
        THEN TIMESTAMP_SECONDS(SAFE_CAST(created.createdAt AS INT64))
      ELSE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(created.createdAt AS STRING))
    END as created_at,
    
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
    -- Filter on actual date field (last 90 days)
    AND PARSE_DATE('%Y-%m-%d', date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
)
SELECT * FROM cleaned;

-- Tạo bảng bill_products từ bill_products_raw (đã tách riêng trong Bronze)
-- Note: created_at sẽ được lấy từ bills table sau khi bills đã được tạo
CREATE OR REPLACE TABLE `sync-nhanhvn-project.silver.bill_products`
PARTITION BY DATE(created_at)
CLUSTER BY bill_id, product_id
AS
SELECT
  CAST(bp.bill_id AS INT64) as bill_id,
  COALESCE(b.created_at, CURRENT_TIMESTAMP()) as created_at,
  CAST(bp.id AS INT64) as product_id,
  CAST(bp.code AS STRING) as product_code,
  CAST(bp.name AS STRING) as product_name,
  CAST(bp.quantity AS FLOAT64) as quantity,
  CAST(bp.price AS FLOAT64) as unit_price,
  CAST(bp.discount AS FLOAT64) as discount_amount,
  CAST(bp.amount AS FLOAT64) as line_amount,
  CAST(bp.vat.percent AS INT64) as vat_percent,
  CAST(bp.vat.amount AS FLOAT64) as vat_amount,
  CAST(bp.imexId AS INT64) as imex_id,
  CURRENT_TIMESTAMP() as processed_at
FROM `sync-nhanhvn-project.bronze.nhanh_bill_products_raw` bp
LEFT JOIN `sync-nhanhvn-project.silver.bills` b
  ON b.bill_id = CAST(bp.bill_id AS INT64)
WHERE bp.bill_id IS NOT NULL
  AND bp.id IS NOT NULL
  AND bp.quantity > 0
  AND bp.price >= 0;

