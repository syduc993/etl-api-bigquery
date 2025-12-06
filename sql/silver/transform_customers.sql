-- Transformation từ Bronze sang Silver cho Customers
-- File này chứa SQL để clean và transform data từ bronze.customers_raw
-- sang silver.customers với các bước:
-- 1. Type casting
-- 2. Deduplication
-- 3. Data validation

CREATE OR REPLACE TABLE `sync-nhanhvn-project.silver.customers`
PARTITION BY DATE(updated_at)
CLUSTER BY id
AS
WITH raw_customers AS (
  SELECT
    *,
    _FILE_NAME as source_file
  FROM `sync-nhanhvn-project.bronze.customers_raw`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id 
      ORDER BY updatedAt DESC
    ) as rn
  FROM raw_customers
),
cleaned AS (
  SELECT
    -- Primary fields
    CAST(id AS INT64) as customer_id,
    CAST(name AS STRING) as customer_name,
    CAST(mobile AS STRING) as mobile,
    CAST(email AS STRING) as email,
    CAST(address AS STRING) as address,
    
    -- Points
    CAST(points AS FLOAT64) as points_balance,
    
    -- Dates
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', createdAt) as created_at,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', updatedAt) as updated_at,
    
    -- Metadata
    source_file,
    CURRENT_TIMESTAMP() as processed_at
    
  FROM deduplicated
  WHERE rn = 1  -- Chỉ lấy record mới nhất cho mỗi customer_id
    AND id IS NOT NULL  -- Validate required fields
    AND name IS NOT NULL
)
SELECT * FROM cleaned;

