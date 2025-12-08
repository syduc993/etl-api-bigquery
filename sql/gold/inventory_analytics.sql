-- Inventory Analytics
-- File này tính toán các metrics về tồn kho và sản phẩm
-- Bao gồm: tốc độ bán, dự báo hết hàng, etc.
-- Note: Không có inventory data từ products table, chỉ tính toán dựa trên sales velocity

CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.inventory_analytics`
CLUSTER BY product_id
AS
WITH product_sales_velocity AS (
  SELECT
    product_id,
    AVG(total_quantity_sold) as avg_daily_sales,
    STDDEV(total_quantity_sold) as stddev_daily_sales,
    MAX(total_quantity_sold) as max_daily_sales,
    MIN(sale_date) as first_sale_date,
    MAX(sale_date) as last_sale_date
  FROM `sync-nhanhvn-project.gold.product_sales_metrics`
  WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY product_id
),
product_info AS (
  SELECT DISTINCT
    product_id,
    product_name,
    product_code
  FROM `sync-nhanhvn-project.gold.product_sales_metrics`
)
SELECT
  pi.product_id,
  pi.product_name,
  pi.product_code,
  COALESCE(psv.avg_daily_sales, 0) as avg_daily_sales,
  COALESCE(psv.max_daily_sales, 0) as max_daily_sales,
  COALESCE(psv.stddev_daily_sales, 0) as stddev_daily_sales,
  psv.first_sale_date,
  psv.last_sale_date,
  DATE_DIFF(CURRENT_DATE(), psv.last_sale_date, DAY) as days_since_last_sale,
  -- Phân loại sản phẩm dựa trên tốc độ bán
  CASE
    WHEN psv.avg_daily_sales = 0 OR psv.last_sale_date IS NULL THEN 'No Sales'
    WHEN DATE_DIFF(CURRENT_DATE(), psv.last_sale_date, DAY) > 90 THEN 'Inactive'
    WHEN psv.avg_daily_sales >= 10 THEN 'Fast Moving'
    WHEN psv.avg_daily_sales >= 5 THEN 'Medium Moving'
    WHEN psv.avg_daily_sales > 0 THEN 'Slow Moving'
    ELSE 'No Sales'
  END as product_status,
  CURRENT_TIMESTAMP() as updated_at
FROM product_info pi
LEFT JOIN product_sales_velocity psv ON pi.product_id = psv.product_id;

