"""Check data for specific date."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from google.cloud import bigquery
from src.config import settings

target_date = "2025-11-29"
client = bigquery.Client(project=settings.gcp_project)

# Check bills
bills_query = f"""
SELECT COUNT(*) as cnt 
FROM `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0`
WHERE date = '{target_date}'
"""
bills_result = list(client.query(bills_query).result())
bills_count = bills_result[0].cnt if bills_result else 0

# Check products by joining with bills
products_query = f"""
SELECT COUNT(*) as cnt
FROM `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0` p
INNER JOIN `{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0` b
  ON p.bill_id = b.id
WHERE b.date = '{target_date}'
"""
products_result = list(client.query(products_query).result())
products_count = products_result[0].cnt if products_result else 0

print(f"📊 Data for {target_date}:")
print(f"   Bills: {bills_count:,} rows")
print(f"   Products: {products_count:,} rows")

