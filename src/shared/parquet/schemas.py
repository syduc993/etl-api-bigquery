"""
Parquet schema definitions for enforcing consistent data types across Parquet files.

This module provides explicit PyArrow schemas to prevent schema evolution issues
when writing Parquet files. Using explicit schemas ensures:
- Consistent types across all files (e.g., discount is always FLOAT64, never INT64)
- Type coercion happens automatically (int -> float conversion)
- Better compatibility with BigQuery external tables
- Early error detection for incompatible data
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
])


# Schema registry - maps entity paths to schemas
# Format: "{platform}/{entity}" -> schema
SCHEMA_REGISTRY: Dict[str, pa.Schema] = {
    'nhanh/bill_products': BILL_PRODUCTS_SCHEMA,
    # Add more schemas as needed:
    # 'nhanh/bills': BILLS_SCHEMA,
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

