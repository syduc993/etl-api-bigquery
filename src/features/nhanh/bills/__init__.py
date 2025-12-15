"""
Bills Feature - Xử lý hóa đơn từ Nhanh API.

Feature này chứa toàn bộ ETL cho bills:
- Extractor: Lấy data từ Nhanh API
- Loader: Flatten nested structures và load trực tiếp vào fact tables
- Pipeline: Orchestrate Extract → Load (flatten integrated)
"""
from .components.extractor import BillExtractor
from .components.loader import BillLoader
from .pipeline import BillPipeline

__all__ = ['BillExtractor', 'BillLoader', 'BillPipeline']
