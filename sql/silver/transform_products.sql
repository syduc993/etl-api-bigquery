-- Transformation từ Bronze sang Silver cho Products
-- File này chứa SQL để clean và transform data từ bronze.products_raw
-- sang silver.products với các bước:
-- 1. Type casting
-- 2. Deduplication
-- 3. Data validation

CREATE OR REPLACE TABLE `sync-nhanhvn-project.silver.products`
PARTITION BY DATE(updated_at)
CLUSTER BY id, category_id
AS
WITH raw_products AS (
  SELECT
    *,
    _FILE_NAME as source_file
  FROM `sync-nhanhvn-project.bronze.products_raw`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id 
      ORDER BY updatedAt DESC
    ) as rn
  FROM raw_products
),
cleaned AS (
  SELECT
    -- Primary fields
    CAST(id AS INT64) as product_id,
    CAST(code AS STRING) as product_code,
    CAST(barcode AS STRING) as barcode,
    CAST(name AS STRING) as product_name,
    
    -- Pricing
    CAST(price AS FLOAT64) as price,
    CAST(wholesalePrice AS FLOAT64) as wholesale_price,
    
    -- Category
    CAST(categoryId AS INT64) as category_id,
    CAST(categoryName AS STRING) as category_name,
    
    -- Inventory
    CAST(inventory AS FLOAT64) as inventory_quantity,
    
    -- Status
    CAST(status AS INT64) as status,
    
    -- Dates
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', createdAt) as created_at,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', updatedAt) as updated_at,
    
    -- Metadata
    source_file,
    CURRENT_TIMESTAMP() as processed_at
    
  FROM deduplicated
  WHERE rn = 1  -- Chỉ lấy record mới nhất cho mỗi product_id
    AND id IS NOT NULL  -- Validate required fields
    AND name IS NOT NULL
)
SELECT * FROM cleaned;

