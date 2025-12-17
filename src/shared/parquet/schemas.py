"""
Parquet schema definitions for enforcing consistent data types across Parquet files.

Module này cung cấp explicit PyArrow schemas để tránh schema evolution issues
khi write Parquet files. Sử dụng explicit schemas đảm bảo:
- Consistent types across all files (ví dụ: discount luôn là FLOAT64, không bao giờ INT64)
- Type coercion tự động (int -> float conversion)
- Tương thích tốt hơn với BigQuery external tables
- Phát hiện lỗi sớm cho incompatible data
- Timestamp fields luôn dùng microsecond precision (pa.timestamp('us')) để tương thích BigQuery

Schemas được register trong SCHEMA_REGISTRY và tự động lookup qua get_schema(entity_path).
"""
import pyarrow as pa
from typing import Dict, Optional


# Schema for nhanh/bill_products
# Based on structure from extractor and BigQuery target schema
BILL_PRODUCTS_SCHEMA = pa.schema([
    # Bill reference
    pa.field('bill_id', pa.int64()),
    
    # Product identification
    pa.field('id', pa.int64()),  # Product ID from API
    pa.field('product_id', pa.int64(), nullable=True),  # Alternative product_id field
    pa.field('code', pa.string(), nullable=True),
    pa.field('barcode', pa.string(), nullable=True),
    pa.field('name', pa.string(), nullable=True),
    
    # Transaction fields - MUST be FLOAT64 to match BigQuery schema
    pa.field('quantity', pa.float64(), nullable=True),
    pa.field('price', pa.float64(), nullable=True),
    pa.field('discount', pa.float64(), nullable=True),  # Critical: always float64, never int64
    pa.field('amount', pa.float64(), nullable=True),
    
    # VAT as nested struct (matches API structure and BigQuery STRUCT type)
    pa.field('vat', pa.struct([
        pa.field('percent', pa.int64(), nullable=True),
        pa.field('amount', pa.float64(), nullable=True)
    ]), nullable=True),
    
    # Additional fields that may exist in API response
    pa.field('imexId', pa.int64(), nullable=True),
    pa.field('imeiId', pa.int64(), nullable=True),
    pa.field('imei', pa.list_(pa.string()), nullable=True),
    pa.field('extendedWarrantyAmount', pa.float64(), nullable=True),
    pa.field('giftProducts', pa.list_(pa.string()), nullable=True),
    
    # Metadata
    pa.field('extraction_timestamp', pa.timestamp('us'), nullable=True),  # TIMESTAMP với microsecond precision (BigQuery compatible)
])


# Schema for nhanh/bills
# Based on flattened structure from BillLoader._flatten_bill() matching fact_sales_bills_v3_0 schema
BILLS_SCHEMA = pa.schema([
    # Basic fields
    pa.field('id', pa.int64()),
    pa.field('depotId', pa.int64(), nullable=True),
    pa.field('date', pa.date32(), nullable=True),  # DATE field
    pa.field('type', pa.int64(), nullable=True),
    pa.field('mode', pa.int64(), nullable=True),
    
    # Customer fields (flattened from customer object)
    pa.field('customer_id', pa.int64(), nullable=True),
    pa.field('customer_name', pa.string(), nullable=True),
    pa.field('customer_mobile', pa.string(), nullable=True),
    pa.field('customer_address', pa.string(), nullable=True),
    
    # Sale/Staff fields (flattened from sale and created objects)
    pa.field('sale_id', pa.int64(), nullable=True),
    pa.field('sale_name', pa.string(), nullable=True),
    pa.field('created_id', pa.int64(), nullable=True),
    pa.field('created_email', pa.string(), nullable=True),
    
    # Payment fields (flattened from payment object) - MUST be FLOAT64
    pa.field('payment_total_amount', pa.float64(), nullable=True),
    pa.field('payment_customer_amount', pa.float64(), nullable=True),
    pa.field('payment_discount', pa.float64(), nullable=True),
    pa.field('payment_points', pa.float64(), nullable=True),
    
    # Payment methods (flattened from nested payment methods)
    pa.field('payment_cash_amount', pa.float64(), nullable=True),
    pa.field('payment_transfer_amount', pa.float64(), nullable=True),
    pa.field('payment_transfer_account_id', pa.int64(), nullable=True),
    pa.field('payment_credit_amount', pa.float64(), nullable=True),
    
    # Other fields
    pa.field('description', pa.string(), nullable=True),
    pa.field('extraction_timestamp', pa.timestamp('us'), nullable=True),  # TIMESTAMP với microsecond precision (BigQuery compatible)
])


# Schema registry - maps entity paths to schemas
# Format: "{platform}/{entity}" -> schema
SCHEMA_REGISTRY: Dict[str, pa.Schema] = {
    'nhanh/bill_products': BILL_PRODUCTS_SCHEMA,
    'nhanh/bills': BILLS_SCHEMA,
    # Add more schemas as needed:
    # 'nhanh/products': PRODUCTS_SCHEMA,
    # 'nhanh/customers': CUSTOMERS_SCHEMA,
}


def get_schema(entity_path: str) -> Optional[pa.Schema]:
    """
    Get explicit schema for an entity path.
    
    Args:
        entity_path: Entity path in format "{platform}/{entity}" (e.g., "nhanh/bill_products")
        
    Returns:
        PyArrow schema if registered, None otherwise (fallback to inference)
    """
    return SCHEMA_REGISTRY.get(entity_path)


def register_schema(entity_path: str, schema: pa.Schema):
    """
    Register a schema for an entity path.
    
    Args:
        entity_path: Entity path in format "{platform}/{entity}"
        schema: PyArrow schema to use
    """
    SCHEMA_REGISTRY[entity_path] = schema

