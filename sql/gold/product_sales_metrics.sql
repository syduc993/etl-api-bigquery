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
    bp.product_id,
    p.product_name,
    p.product_code,
    p.category_id,
    p.category_name,
    bp.quantity,
    bp.unit_price,
    bp.line_amount,
    bp.discount_amount,
    b.bill_mode,
    b.customer_id
  FROM `sync-nhanhvn-project.silver.bill_products` bp
  INNER JOIN `sync-nhanhvn-project.silver.bills` b
    ON bp.bill_id = b.bill_id
  LEFT JOIN `sync-nhanhvn-project.silver.products` p
    ON bp.product_id = p.product_id
  WHERE b.bill_type = 2  -- Chỉ tính xuất kho (bán hàng)
    AND bp.quantity > 0
    AND bp.line_amount > 0
)
SELECT
  sale_date,
  product_id,
  product_name,
  product_code,
  category_id,
  category_name,
  bill_mode,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT bill_id) as times_sold,
  SUM(quantity) as total_quantity_sold,
  SUM(line_amount) as total_revenue,
  AVG(unit_price) as avg_selling_price,
  SUM(discount_amount) as total_discount,
  MIN(unit_price) as min_price,
  MAX(unit_price) as max_price,
  CURRENT_TIMESTAMP() as updated_at
FROM product_sales
GROUP BY sale_date, product_id, product_name, product_code, category_id, category_name, bill_mode;

-- Tạo bảng tổng hợp theo product (không phân ngày)
CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.product_summary`
CLUSTER BY product_id
AS
SELECT
  product_id,
  product_name,
  product_code,
  category_id,
  category_name,
  COUNT(DISTINCT sale_date) as days_sold,
  COUNT(DISTINCT customer_id) as total_unique_customers,
  SUM(times_sold) as total_times_sold,
  SUM(total_quantity_sold) as total_quantity_sold,
  SUM(total_revenue) as total_revenue,
  AVG(avg_selling_price) as overall_avg_price,
  SUM(total_discount) as total_discount,
  MIN(sale_date) as first_sale_date,
  MAX(sale_date) as last_sale_date,
  CURRENT_TIMESTAMP() as updated_at
FROM `sync-nhanhvn-project.gold.product_sales_metrics`
GROUP BY product_id, product_name, product_code, category_id, category_name;

