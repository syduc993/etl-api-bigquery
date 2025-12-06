"""
Extractor cho sản phẩm (products) từ Nhanh API.
File này xử lý việc lấy danh sách sản phẩm với hỗ trợ:
- Full sync: Lấy tất cả sản phẩm
- Incremental sync: Chỉ lấy sản phẩm đã thay đổi dựa trên updatedAt
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class ProductExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho sản phẩm từ Nhanh API.
    
    Hỗ trợ cả full sync và incremental sync dựa trên updatedAt.
    """
    
    def __init__(self):
        """
        Khởi tạo ProductExtractor.
        
        Kế thừa từ cả BaseExtractor (cho registry) và NhanhApiClient (cho API calls).
        """
        BaseExtractor.__init__(self, platform="nhanh", entity="products")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract products - wrapper cho fetch_products để implement BaseExtractor interface.
        
        Args:
            **kwargs: Các tham số cho fetch_products
            
        Returns:
            List[Dict[str, Any]]: Danh sách products
        """
        return self.fetch_products(**kwargs)
    
    def get_schema(self) -> Dict:
        """
        Lấy schema của products entity.
        
        Returns:
            Dict: Schema definition cho products
        """
        return {
            "entity": "products",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "code", "type": "STRING", "required": False},
                {"name": "barcode", "type": "STRING", "required": False},
                {"name": "name", "type": "STRING", "required": True},
                {"name": "price", "type": "FLOAT64", "required": False},
                {"name": "categoryId", "type": "INT64", "required": False},
            ]
        }
    
    def fetch_products(
        self,
        name: Optional[str] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None,
        incremental: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách sản phẩm (full sync hoặc incremental).
        
        Args:
            name: Lọc theo tên sản phẩm (partial match)
            updated_at_from: Ngày bắt đầu cho filter updatedAt (cho incremental)
            updated_at_to: Ngày kết thúc cho filter updatedAt (cho incremental)
            incremental: True để dùng updatedAt filters, False để lấy tất cả
            
        Returns:
            List[Dict[str, Any]]: Danh sách các sản phẩm
            
        Raises:
            Exception: Nếu có lỗi khi gọi API
        """
        # Build filters
        filters: Dict[str, Any] = {}
        
        if name:
            filters["name"] = name
        
        if incremental and updated_at_from and updated_at_to:
            # Incremental extraction using updatedAt
            filters["updatedAtFrom"] = updated_at_from.isoformat()
            filters["updatedAtTo"] = updated_at_to.isoformat()
            logger.info(
                "Fetching products incrementally",
                from_date=updated_at_from.isoformat(),
                to_date=updated_at_to.isoformat()
            )
        elif not incremental:
            logger.info("Fetching all products (full sync)")
        
        # Build request body
        body = {
            "filters": filters,
            "paginator": {
                "size": 50
            }
        }
        
        try:
            products = self.fetch_paginated("/product/list", body)
            logger.info(f"Fetched {len(products)} products", total_products=len(products))
            return products
        
        except Exception as e:
            logger.error(f"Error fetching products", error=str(e))
            raise

