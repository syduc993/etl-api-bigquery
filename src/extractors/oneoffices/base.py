"""
Base client cho 1Offices API.
File này là template/base class cho 1Offices extractors.
Có thể mở rộng sau khi có 1Offices API credentials và documentation.
"""
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.utils.logging import get_logger

logger = get_logger(__name__)


class OneOfficesApiClient:
    """
    Base client cho 1Offices API.
    
    Template class - cần implement sau khi có 1Offices API documentation.
    """
    
    def __init__(self):
        """
        Khởi tạo 1Offices API client.
        
        TODO: Load credentials từ Secret Manager
        TODO: Setup authentication
        TODO: Setup rate limiting
        """
        # TODO: Implement 1Offices API client
        pass
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Thực hiện 1Offices API request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Dict[str, Any]: API response
            
        TODO: Implement 1Offices API request logic
        """
        # TODO: Implement
        raise NotImplementedError("1Offices API client chưa được implement")
    
    def fetch_paginated(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Lấy tất cả pages từ 1Offices API.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            List[Dict[str, Any]]: Danh sách records
            
        TODO: Implement 1Offices pagination logic
        """
        # TODO: Implement
        raise NotImplementedError("1Offices pagination chưa được implement")


class OneOfficesOrderExtractor(BaseExtractor, OneOfficesApiClient):
    """
    Extractor cho đơn hàng từ 1Offices.
    
    Template class - cần implement sau khi có 1Offices API documentation.
    """
    
    def __init__(self):
        """Khởi tạo OneOfficesOrderExtractor."""
        BaseExtractor.__init__(self, platform="oneoffices", entity="orders")
        OneOfficesApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract orders từ 1Offices.
        
        Args:
            **kwargs: Các tham số
            
        Returns:
            List[Dict[str, Any]]: Danh sách orders
            
        TODO: Implement 1Offices order extraction
        """
        # TODO: Implement
        raise NotImplementedError("1Offices order extraction chưa được implement")
    
    def get_schema(self) -> Dict:
        """Lấy schema của 1Offices orders entity."""
        return {
            "entity": "orders",
            "platform": "oneoffices",
            "fields": [
                # TODO: Define schema sau khi có 1Offices API documentation
            ]
        }

