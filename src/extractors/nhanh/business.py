"""
Extractors cho Business entities từ Nhanh API.
File này chứa các extractors cho:
- Depots (kho hàng)
- Users (nhân viên)
- Suppliers (nhà cung cấp)
"""
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class DepotExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho kho hàng (depots) từ Nhanh API.
    """
    
    def __init__(self):
        """Khởi tạo DepotExtractor."""
        BaseExtractor.__init__(self, platform="nhanh", entity="depots")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract depots.
        
        Args:
            **kwargs: Các tham số (hiện tại không có filters)
            
        Returns:
            List[Dict[str, Any]]: Danh sách depots
        """
        body = {
            "paginator": {"size": 50}
        }
        
        depots = self.fetch_paginated("/business/depot", body)
        logger.info(f"Fetched {len(depots)} depots", total_depots=len(depots))
        return depots
    
    def get_schema(self) -> Dict:
        """Lấy schema của depots entity."""
        return {
            "entity": "depots",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "name", "type": "STRING", "required": True},
            ]
        }


class UserExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho nhân viên (users) từ Nhanh API.
    """
    
    def __init__(self):
        """Khởi tạo UserExtractor."""
        BaseExtractor.__init__(self, platform="nhanh", entity="users")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract users.
        
        Args:
            **kwargs: Các tham số (hiện tại không có filters)
            
        Returns:
            List[Dict[str, Any]]: Danh sách users
        """
        body = {
            "paginator": {"size": 50}
        }
        
        users = self.fetch_paginated("/business/user", body)
        logger.info(f"Fetched {len(users)} users", total_users=len(users))
        return users
    
    def get_schema(self) -> Dict:
        """Lấy schema của users entity."""
        return {
            "entity": "users",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "name", "type": "STRING", "required": True},
            ]
        }


class SupplierExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho nhà cung cấp (suppliers) từ Nhanh API.
    """
    
    def __init__(self):
        """Khởi tạo SupplierExtractor."""
        BaseExtractor.__init__(self, platform="nhanh", entity="suppliers")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract suppliers.
        
        Args:
            **kwargs: Các tham số (hiện tại không có filters)
            
        Returns:
            List[Dict[str, Any]]: Danh sách suppliers
        """
        body = {
            "paginator": {"size": 50}
        }
        
        suppliers = self.fetch_paginated("/business/supplier", body)
        logger.info(f"Fetched {len(suppliers)} suppliers", total_suppliers=len(suppliers))
        return suppliers
    
    def get_schema(self) -> Dict:
        """Lấy schema của suppliers entity."""
        return {
            "entity": "suppliers",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "name", "type": "STRING", "required": True},
            ]
        }

