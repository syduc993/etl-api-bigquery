"""
Extractor cho đơn hàng (orders) từ Nhanh API.
File này xử lý việc lấy danh sách đơn hàng với hỗ trợ:
- Full sync và incremental sync
- Các filters: status, source, date range
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class OrderExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho đơn hàng từ Nhanh API.
    
    Hỗ trợ cả full sync và incremental sync dựa trên updatedAt.
    """
    
    def __init__(self):
        """
        Khởi tạo OrderExtractor.
        
        Kế thừa từ cả BaseExtractor (cho registry) và NhanhApiClient (cho API calls).
        """
        BaseExtractor.__init__(self, platform="nhanh", entity="orders")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract orders - wrapper cho fetch_orders để implement BaseExtractor interface.
        
        Args:
            **kwargs: Các tham số cho fetch_orders
            
        Returns:
            List[Dict[str, Any]]: Danh sách orders
        """
        return self.fetch_orders(**kwargs)
    
    def get_schema(self) -> Dict:
        """
        Lấy schema của orders entity.
        
        Returns:
            Dict: Schema definition cho orders
        """
        return {
            "entity": "orders",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "orderId", "type": "INT64", "required": False},
                {"name": "status", "type": "INT64", "required": False},
                {"name": "source", "type": "STRING", "required": False},
            ]
        }
    
    def fetch_orders(
        self,
        status: Optional[int] = None,
        source: Optional[str] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None,
        incremental: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách đơn hàng (full sync hoặc incremental).
        
        Args:
            status: Lọc theo status
            source: Lọc theo nguồn đơn hàng
            updated_at_from: Ngày bắt đầu cho filter updatedAt (cho incremental)
            updated_at_to: Ngày kết thúc cho filter updatedAt (cho incremental)
            incremental: True để dùng updatedAt filters, False để lấy tất cả
            
        Returns:
            List[Dict[str, Any]]: Danh sách các đơn hàng
            
        Raises:
            Exception: Nếu có lỗi khi gọi API
        """
        # Build filters
        filters: Dict[str, Any] = {}
        
        if status:
            filters["status"] = status
        if source:
            filters["source"] = source
        
        if incremental and updated_at_from and updated_at_to:
            # Incremental extraction using updatedAt
            filters["updatedAtFrom"] = updated_at_from.isoformat()
            filters["updatedAtTo"] = updated_at_to.isoformat()
            logger.info(
                "Fetching orders incrementally",
                from_date=updated_at_from.isoformat(),
                to_date=updated_at_to.isoformat()
            )
        elif not incremental:
            logger.info("Fetching all orders (full sync)")
        
        # Build request body
        body = {
            "filters": filters,
            "paginator": {
                "size": 50
            }
        }
        
        try:
            orders = self.fetch_paginated("/order/list", body)
            logger.info(f"Fetched {len(orders)} orders", total_orders=len(orders))
            return orders
        
        except Exception as e:
            logger.error(f"Error fetching orders", error=str(e))
            raise

