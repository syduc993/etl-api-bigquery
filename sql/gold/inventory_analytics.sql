-- Inventory Analytics
-- File này tính toán các metrics về tồn kho và sản phẩm
-- Bao gồm: tồn kho hiện tại, tốc độ bán, dự báo hết hàng, etc.

CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.inventory_analytics`
CLUSTER BY product_id
AS
WITH product_sales_velocity AS (
  SELECT
    product_id,
    AVG(total_quantity_sold) as avg_daily_sales,
    STDDEV(total_quantity_sold) as stddev_daily_sales,
    MAX(total_quantity_sold) as max_daily_sales
  FROM `sync-nhanhvn-project.gold.product_sales_metrics`
  WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY product_id
),
current_inventory AS (
  SELECT
    product_id,
    inventory_quantity,
    updated_at as inventory_updated_at
  FROM `sync-nhanhvn-project.silver.products`
  WHERE inventory_quantity IS NOT NULL
)
SELECT
  p.product_id,
  p.product_name,
  p.product_code,
  p.category_name,
  COALESCE(ci.inventory_quantity, 0) as current_inventory,
  COALESCE(psv.avg_daily_sales, 0) as avg_daily_sales,
  COALESCE(psv.max_daily_sales, 0) as max_daily_sales,
  -- Tính số ngày còn lại (dựa trên tốc độ bán trung bình)
  CASE
    WHEN psv.avg_daily_sales > 0 THEN
      CAST(COALESCE(ci.inventory_quantity, 0) / psv.avg_daily_sales AS INT64)
    ELSE NULL
  END as estimated_days_remaining,
  -- Cảnh báo hết hàng
  CASE
    WHEN ci.inventory_quantity IS NULL OR ci.inventory_quantity = 0 THEN 'Out of Stock'
    WHEN ci.inventory_quantity <= psv.max_daily_sales THEN 'Low Stock'
    WHEN ci.inventory_quantity <= psv.avg_daily_sales * 7 THEN 'Reorder Soon'
    ELSE 'In Stock'
  END as stock_status,
  ci.inventory_updated_at,
  CURRENT_TIMESTAMP() as updated_at
FROM `sync-nhanhvn-project.silver.products` p
LEFT JOIN current_inventory ci ON p.product_id = ci.product_id
LEFT JOIN product_sales_velocity psv ON p.product_id = psv.product_id;

