"""
Nhanh API extractors.
Module này chứa tất cả extractors cho Nhanh.vn API.
Tự động đăng ký tất cả extractors vào registry khi import.
"""
from src.extractors.registry import registry, BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.extractors.nhanh.bill import BillExtractor
from src.extractors.nhanh.product import ProductExtractor
from src.extractors.nhanh.customer import CustomerExtractor
from src.extractors.nhanh.order import OrderExtractor
from src.extractors.nhanh.business import (
    DepotExtractor,
    UserExtractor,
    SupplierExtractor
)

# Đăng ký tất cả Nhanh extractors
PLATFORM = "nhanh"

registry.register(PLATFORM, "bills", BillExtractor)
registry.register(PLATFORM, "products", ProductExtractor)
registry.register(PLATFORM, "customers", CustomerExtractor)
registry.register(PLATFORM, "orders", OrderExtractor)
registry.register(PLATFORM, "depots", DepotExtractor)
registry.register(PLATFORM, "users", UserExtractor)
registry.register(PLATFORM, "suppliers", SupplierExtractor)

# Export để backward compatibility
__all__ = [
    "NhanhApiClient",
    "BillExtractor",
    "ProductExtractor",
    "CustomerExtractor",
    "OrderExtractor",
    "DepotExtractor",
    "UserExtractor",
    "SupplierExtractor",
    "registry",
    "PLATFORM"
]

