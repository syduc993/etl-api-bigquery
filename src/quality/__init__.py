"""
Quality module cho data validation và quality checks.
Package này cung cấp:
- Schema validation với Pydantic
- Data quality checks (nulls, duplicates, types)
"""
from src.quality.validators import (
    BillRecord,
    ProductRecord,
    CustomerRecord,
    validate_records,
    ValidationResult
)
from src.quality.checks import DataQualityChecker, QualityReport

__all__ = [
    'BillRecord',
    'ProductRecord',
    'CustomerRecord',
    'validate_records',
    'ValidationResult',
    'DataQualityChecker',
    'QualityReport'
]
