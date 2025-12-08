-- Customer Lifetime Value (CLV)
-- File này tính toán giá trị khách hàng trong suốt thời gian
-- Bao gồm: tổng chi tiêu, số đơn hàng, đơn hàng cuối cùng, etc.

CREATE OR REPLACE TABLE `sync-nhanhvn-project.gold.customer_lifetime_value`
PARTITION BY first_order_date
CLUSTER BY customer_id
AS
WITH customer_orders AS (
  SELECT
    customer_id,
    customer_name,
    customer_mobile,
    bill_id,
    bill_date,
    total_amount,
    discount_amount,
    points_earned,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY bill_date ASC) as order_sequence,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY bill_date DESC) as reverse_order_sequence
  FROM `sync-nhanhvn-project.silver.bills`
  WHERE customer_id IS NOT NULL
    AND bill_type = 2  -- Chỉ tính xuất kho (bán hàng)
    AND total_amount > 0
),
customer_stats AS (
  SELECT
    customer_id,
    customer_name,
    customer_mobile,
    COUNT(DISTINCT bill_id) as total_orders,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value,
    MIN(bill_date) as first_order_date,
    MAX(bill_date) as last_order_date,
    DATE_DIFF(MAX(bill_date), MIN(bill_date), DAY) as customer_lifetime_days,
    SUM(discount_amount) as total_discount_received,
    SUM(points_earned) as total_points_earned,
    -- Tính số ngày từ đơn hàng cuối
    DATE_DIFF(CURRENT_DATE(), MAX(bill_date), DAY) as days_since_last_order
  FROM customer_orders
  GROUP BY customer_id, customer_name, customer_mobile
)
SELECT
  cs.*,
  -- Tính CLV (có thể customize theo business logic)
  cs.total_spent / NULLIF(cs.customer_lifetime_days, 0) * 365 as estimated_annual_value,
  -- Phân loại khách hàng
  CASE
    WHEN cs.days_since_last_order <= 30 THEN 'Active'
    WHEN cs.days_since_last_order <= 90 THEN 'At Risk'
    WHEN cs.days_since_last_order <= 180 THEN 'Inactive'
    ELSE 'Churned'
  END as customer_status,
  CASE
    WHEN cs.total_spent >= 10000000 THEN 'VIP'
    WHEN cs.total_spent >= 5000000 THEN 'Gold'
    WHEN cs.total_spent >= 2000000 THEN 'Silver'
    ELSE 'Bronze'
  END as customer_tier,
  CURRENT_TIMESTAMP() as updated_at
FROM customer_stats cs;

