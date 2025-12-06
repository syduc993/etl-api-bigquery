-- Daily Revenue Summary
-- File này tạo bảng tổng hợp doanh thu theo ngày từ Silver layer
-- Bao gồm: tổng doanh thu, số đơn hàng, giá trị trung bình đơn hàng, etc.

CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.daily_revenue_summary`
PARTITION BY sale_date
CLUSTER BY bill_mode
AS
SELECT
  bill_date as sale_date,
  bill_mode,
  COUNT(DISTINCT bill_id) as total_orders,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(total_amount) as total_revenue,
  AVG(total_amount) as avg_order_value,
  SUM(discount_amount) as total_discount,
  SUM(points_earned) as total_points_earned,
  SUM(points_used) as total_points_used,
  MIN(total_amount) as min_order_value,
  MAX(total_amount) as max_order_value,
  CURRENT_TIMESTAMP() as updated_at
FROM `sync-nhanhvn-project.silver.bills`
WHERE bill_type = 2  -- Chỉ tính xuất kho (bán hàng)
  AND total_amount > 0
GROUP BY sale_date, bill_mode;

-- Tạo materialized view cho query performance
CREATE OR REPLACE MATERIALIZED VIEW `sync-nhanhvn-project.gold.daily_revenue_summary_mv`
PARTITION BY sale_date
CLUSTER BY bill_mode
AS
SELECT * FROM `sync-nhanhvn-project.gold.daily_revenue_summary`;

