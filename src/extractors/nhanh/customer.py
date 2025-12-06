"""
Extractor cho khách hàng (customers) từ Nhanh API.
File này xử lý việc lấy danh sách khách hàng với hỗ trợ:
- Full sync: Lấy tất cả khách hàng
- Incremental sync: Chỉ lấy khách hàng đã thay đổi dựa trên updatedAt
- Filter theo name hoặc mobile
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class CustomerExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho khách hàng từ Nhanh API.
    
    Hỗ trợ cả full sync và incremental sync dựa trên updatedAt.
    """
    
    def __init__(self):
        """
        Khởi tạo CustomerExtractor.
        
        Kế thừa từ cả BaseExtractor (cho registry) và NhanhApiClient (cho API calls).
        """
        BaseExtractor.__init__(self, platform="nhanh", entity="customers")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract customers - wrapper cho fetch_customers để implement BaseExtractor interface.
        
        Args:
            **kwargs: Các tham số cho fetch_customers
            
        Returns:
            List[Dict[str, Any]]: Danh sách customers
        """
        return self.fetch_customers(**kwargs)
    
    def get_schema(self) -> Dict:
        """
        Lấy schema của customers entity.
        
        Returns:
            Dict: Schema definition cho customers
        """
        return {
            "entity": "customers",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "name", "type": "STRING", "required": True},
                {"name": "mobile", "type": "STRING", "required": False},
                {"name": "email", "type": "STRING", "required": False},
                {"name": "address", "type": "STRING", "required": False},
            ]
        }
    
    def fetch_customers(
        self,
        name: Optional[str] = None,
        mobile: Optional[str] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None,
        incremental: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách khách hàng (full sync hoặc incremental).
        
        Args:
            name: Lọc theo tên khách hàng (partial match)
            mobile: Lọc theo số điện thoại khách hàng
            updated_at_from: Ngày bắt đầu cho filter updatedAt (cho incremental)
            updated_at_to: Ngày kết thúc cho filter updatedAt (cho incremental)
            incremental: True để dùng updatedAt filters, False để lấy tất cả
            
        Returns:
            List[Dict[str, Any]]: Danh sách các khách hàng
            
        Raises:
            Exception: Nếu có lỗi khi gọi API
        """
        # Build filters
        filters: Dict[str, Any] = {}
        
        if name:
            filters["name"] = name
        if mobile:
            filters["mobile"] = mobile
        
        if incremental and updated_at_from and updated_at_to:
            # Incremental extraction using updatedAt
            filters["updatedAtFrom"] = updated_at_from.isoformat()
            filters["updatedAtTo"] = updated_at_to.isoformat()
            logger.info(
                "Fetching customers incrementally",
                from_date=updated_at_from.isoformat(),
                to_date=updated_at_to.isoformat()
            )
        elif not incremental:
            logger.info("Fetching all customers (full sync)")
        
        # Build request body
        body = {
            "filters": filters,
            "paginator": {
                "size": 50
            }
        }
        
        try:
            customers = self.fetch_paginated("/customer/list", body)
            logger.info(f"Fetched {len(customers)} customers", total_customers=len(customers))
            return customers
        
        except Exception as e:
            logger.error(f"Error fetching customers", error=str(e))
            raise

