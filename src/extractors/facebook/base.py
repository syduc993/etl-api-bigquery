"""
Base client cho Facebook API.
File này là template/base class cho Facebook extractors.
Có thể mở rộng sau khi có Facebook API credentials và documentation.
"""
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.utils.logging import get_logger

logger = get_logger(__name__)


class FacebookApiClient:
    """
    Base client cho Facebook API.
    
    Template class - cần implement sau khi có Facebook API documentation.
    """
    
    def __init__(self):
        """
        Khởi tạo Facebook API client.
        
        TODO: Load credentials từ Secret Manager
        TODO: Setup authentication
        TODO: Setup rate limiting
        """
        # TODO: Implement Facebook API client
        pass
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Thực hiện Facebook API request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Dict[str, Any]: API response
            
        TODO: Implement Facebook API request logic
        """
        # TODO: Implement
        raise NotImplementedError("Facebook API client chưa được implement")
    
    def fetch_paginated(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Lấy tất cả pages từ Facebook API.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            List[Dict[str, Any]]: Danh sách records
            
        TODO: Implement Facebook pagination logic
        """
        # TODO: Implement
        raise NotImplementedError("Facebook pagination chưa được implement")


class FacebookOrderExtractor(BaseExtractor, FacebookApiClient):
    """
    Extractor cho đơn hàng từ Facebook.
    
    Template class - cần implement sau khi có Facebook API documentation.
    """
    
    def __init__(self):
        """Khởi tạo FacebookOrderExtractor."""
        BaseExtractor.__init__(self, platform="facebook", entity="orders")
        FacebookApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract orders từ Facebook.
        
        Args:
            **kwargs: Các tham số
            
        Returns:
            List[Dict[str, Any]]: Danh sách orders
            
        TODO: Implement Facebook order extraction
        """
        # TODO: Implement
        raise NotImplementedError("Facebook order extraction chưa được implement")
    
    def get_schema(self) -> Dict:
        """Lấy schema của Facebook orders entity."""
        return {
            "entity": "orders",
            "platform": "facebook",
            "fields": [
                # TODO: Define schema sau khi có Facebook API documentation
            ]
        }

