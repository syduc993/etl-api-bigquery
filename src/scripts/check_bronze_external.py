"""Check bronze external table data."""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from google.cloud import bigquery
from src.config import settings

client = bigquery.Client(project=settings.gcp_project)

# Check bills
bills_query = f"""
SELECT COUNT(*) as cnt 
FROM `{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bills_raw`
"""
bills_result = list(client.query(bills_query).result())
bills_count = bills_result[0].cnt if bills_result else 0

# Check products
products_query = f"""
SELECT COUNT(*) as cnt 
FROM `{settings.gcp_project}.{settings.bronze_dataset}.nhanh_bill_products_raw`
"""
products_result = list(client.query(products_query).result())
products_count = products_result[0].cnt if products_result else 0

print(f"📊 Bronze External Tables:")
print(f"   nhanh_bills_raw: {bills_count:,} rows")
print(f"   nhanh_bill_products_raw: {products_count:,} rows")

