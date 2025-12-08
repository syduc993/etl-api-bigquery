-- Product Sales Metrics
-- File này tính toán các metrics về sản phẩm từ Silver layer
-- Bao gồm: doanh số, số lượng bán, top products, etc.

CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.product_sales_metrics`
PARTITION BY sale_date
CLUSTER BY product_id
AS
WITH product_sales AS (
  SELECT
    DATE(bp.created_at) as sale_date,
    bp.bill_id,
    bp.product_id,
    bp.product_code,
    bp.product_name,
    bp.quantity,
    bp.unit_price,
    bp.line_amount,
    bp.discount_amount,
    b.bill_mode,
    b.customer_id
  FROM `sync-nhanhvn-project.silver.bill_products` bp
  INNER JOIN `sync-nhanhvn-project.silver.bills` b
    ON bp.bill_id = b.bill_id
  WHERE b.bill_type = 2  -- Chỉ tính xuất kho (bán hàng)
    AND bp.quantity > 0
    AND bp.line_amount > 0
)
SELECT
  sale_date,
  product_id,
  product_name,
  product_code,
  CAST(NULL AS INT64) as category_id,
  CAST(NULL AS STRING) as category_name,
  bill_mode,
  COUNT(DISTINCT ps.customer_id) as unique_customers,
  COUNT(DISTINCT ps.bill_id) as times_sold,
  SUM(ps.quantity) as total_quantity_sold,
  SUM(ps.line_amount) as total_revenue,
  AVG(ps.unit_price) as avg_selling_price,
  SUM(ps.discount_amount) as total_discount,
  MIN(ps.unit_price) as min_price,
  MAX(ps.unit_price) as max_price,
  CURRENT_TIMESTAMP() as updated_at
FROM product_sales ps
GROUP BY sale_date, product_id, product_name, product_code, category_id, category_name, bill_mode;

-- Tạo bảng tổng hợp theo product (không phân ngày)
CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.product_summary`
CLUSTER BY product_id
AS
WITH product_customers AS (
  SELECT
    bp.product_id,
    COUNT(DISTINCT b.customer_id) as total_unique_customers
  FROM `sync-nhanhvn-project.silver.bill_products` bp
  INNER JOIN `sync-nhanhvn-project.silver.bills` b
    ON bp.bill_id = b.bill_id
  WHERE b.bill_type = 2  -- Chỉ tính xuất kho (bán hàng)
    AND b.customer_id IS NOT NULL
  GROUP BY bp.product_id
)
SELECT
  psm.product_id,
  psm.product_name,
  psm.product_code,
  psm.category_id,
  psm.category_name,
  COUNT(DISTINCT psm.sale_date) as days_sold,
  COALESCE(pc.total_unique_customers, 0) as total_unique_customers,
  SUM(psm.times_sold) as total_times_sold,
  SUM(psm.total_quantity_sold) as total_quantity_sold,
  SUM(psm.total_revenue) as total_revenue,
  AVG(psm.avg_selling_price) as overall_avg_price,
  SUM(psm.total_discount) as total_discount,
  MIN(psm.sale_date) as first_sale_date,
  MAX(psm.sale_date) as last_sale_date,
  CURRENT_TIMESTAMP() as updated_at
FROM `sync-nhanhvn-project.gold.product_sales_metrics` psm
LEFT JOIN product_customers pc ON psm.product_id = pc.product_id
GROUP BY psm.product_id, psm.product_name, psm.product_code, psm.category_id, psm.category_name, pc.total_unique_customers;

