"""
Base client cho TikTok API.
File này là template/base class cho TikTok extractors.
Có thể mở rộng sau khi có TikTok API credentials và documentation.
"""
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TikTokApiClient:
    """
    Base client cho TikTok API.
    
    Template class - cần implement sau khi có TikTok API documentation.
    """
    
    def __init__(self):
        """
        Khởi tạo TikTok API client.
        
        TODO: Load credentials từ Secret Manager
        TODO: Setup authentication
        TODO: Setup rate limiting
        """
        # TODO: Implement TikTok API client
        pass
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Thực hiện TikTok API request.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            Dict[str, Any]: API response
            
        TODO: Implement TikTok API request logic
        """
        # TODO: Implement
        raise NotImplementedError("TikTok API client chưa được implement")
    
    def fetch_paginated(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Lấy tất cả pages từ TikTok API.
        
        Args:
            endpoint: API endpoint
            params: Request parameters
            
        Returns:
            List[Dict[str, Any]]: Danh sách records
            
        TODO: Implement TikTok pagination logic
        """
        # TODO: Implement
        raise NotImplementedError("TikTok pagination chưa được implement")


class TikTokOrderExtractor(BaseExtractor, TikTokApiClient):
    """
    Extractor cho đơn hàng từ TikTok.
    
    Template class - cần implement sau khi có TikTok API documentation.
    """
    
    def __init__(self):
        """Khởi tạo TikTokOrderExtractor."""
        BaseExtractor.__init__(self, platform="tiktok", entity="orders")
        TikTokApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract orders từ TikTok.
        
        Args:
            **kwargs: Các tham số
            
        Returns:
            List[Dict[str, Any]]: Danh sách orders
            
        TODO: Implement TikTok order extraction
        """
        # TODO: Implement
        raise NotImplementedError("TikTok order extraction chưa được implement")
    
    def get_schema(self) -> Dict:
        """Lấy schema của TikTok orders entity."""
        return {
            "entity": "orders",
            "platform": "tiktok",
            "fields": [
                # TODO: Define schema sau khi có TikTok API documentation
            ]
        }

