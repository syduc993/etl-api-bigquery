"""
Extractors module - Quản lý extractors từ nhiều platforms.
Module này cung cấp registry và base classes để dễ dàng mở rộng
cho các endpoints và platforms mới.
"""
from src.extractors.registry import registry, BaseExtractor, ExtractorRegistry
from src.extractors.config import (
    get_endpoint_config,
    list_available_entities,
    list_available_platforms,
    PLATFORMS
)

# Import Nhanh extractors để auto-register
from src.extractors import nhanh

# Import other platforms (sẽ implement sau)
# from src.extractors import facebook
# from src.extractors import tiktok
# from src.extractors import oneoffices

__all__ = [
    "registry",
    "BaseExtractor",
    "ExtractorRegistry",
    "get_endpoint_config",
    "list_available_entities",
    "list_available_platforms",
    "PLATFORMS"
]
