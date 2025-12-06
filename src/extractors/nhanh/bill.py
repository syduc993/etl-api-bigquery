"""
Extractor cho hóa đơn (bills) từ Nhanh API.
File này xử lý việc lấy danh sách hóa đơn với các tính năng:
- Tự động chia date range thành các chunks 31 ngày (do API giới hạn)
- Hỗ trợ incremental extraction dựa trên updatedAt
- Hỗ trợ các filters: modes, type, customerId, fromDate/toDate
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class BillExtractor(BaseExtractor, NhanhApiClient):
    """
    Extractor cho hóa đơn/invoices từ Nhanh API.
    
    Extractor này tự động xử lý giới hạn 31 ngày của API bằng cách
    chia date range thành các chunks nhỏ hơn nếu cần.
    """
    
    def __init__(self):
        """
        Khởi tạo BillExtractor.
        
        Kế thừa từ cả BaseExtractor (cho registry) và NhanhApiClient (cho API calls).
        """
        BaseExtractor.__init__(self, platform="nhanh", entity="bills")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract bills - wrapper cho fetch_bills để implement BaseExtractor interface.
        
        Args:
            **kwargs: Các tham số cho fetch_bills
            
        Returns:
            List[Dict[str, Any]]: Danh sách bills
        """
        return self.fetch_bills(**kwargs)
    
    def get_schema(self) -> Dict:
        """
        Lấy schema của bills entity.
        
        Returns:
            Dict: Schema definition cho bills
        """
        return {
            "entity": "bills",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "orderId", "type": "INT64", "required": False},
                {"name": "depotId", "type": "INT64", "required": False},
                {"name": "date", "type": "DATE", "required": True},
                {"name": "type", "type": "INT64", "required": True},
                {"name": "mode", "type": "INT64", "required": True},
                {"name": "customer", "type": "STRUCT", "required": False},
                {"name": "products", "type": "ARRAY", "required": False},
                {"name": "payment", "type": "STRUCT", "required": False},
            ]
        }
    
    def fetch_bills(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        modes: Optional[List[int]] = None,
        bill_type: Optional[int] = None,
        customer_id: Optional[int] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách hóa đơn với xử lý giới hạn 31 ngày.
        
        Hàm này tự động chia date range thành các chunks 31 ngày
        nếu range lớn hơn giới hạn của API.
        
        Args:
            from_date: Ngày bắt đầu cho filter bill date (format: YYYY-MM-DD)
            to_date: Ngày kết thúc cho filter bill date (format: YYYY-MM-DD)
            modes: Danh sách mode IDs (ví dụ: [2] cho bán lẻ)
            bill_type: Loại hóa đơn (1 = nhập kho, 2 = xuất kho)
            customer_id: Lọc theo ID khách hàng
            updated_at_from: Ngày bắt đầu cho filter updatedAt (cho incremental)
            updated_at_to: Ngày kết thúc cho filter updatedAt (cho incremental)
            
        Returns:
            List[Dict[str, Any]]: Danh sách các hóa đơn
            
        Note:
            - Nếu dùng updated_at_from/to thì sẽ ưu tiên incremental extraction
            - Nếu không có date range, mặc định lấy 31 ngày gần nhất
        """
        all_bills = []
        
        # Determine date range
        if updated_at_from and updated_at_to:
            # Use updatedAt for incremental extraction
            date_chunks = self.split_date_range(updated_at_from, updated_at_to)
            date_field = "updatedAtFrom"
            date_to_field = "updatedAtTo"
        elif from_date and to_date:
            # Use fromDate/toDate for bill date filter
            date_chunks = self.split_date_range(from_date, to_date)
            date_field = "fromDate"
            date_to_field = "toDate"
        else:
            # Default: last 31 days
            to_date = datetime.now()
            from_date = to_date - timedelta(days=30)
            date_chunks = [(from_date, to_date)]
            date_field = "fromDate"
            date_to_field = "toDate"
        
        logger.info(
            f"Fetching bills in {len(date_chunks)} date chunks",
            chunks=len(date_chunks),
            date_field=date_field
        )
        
        # Fetch bills for each date chunk
        for chunk_idx, (chunk_from, chunk_to) in enumerate(date_chunks, 1):
            logger.info(
                f"Processing date chunk {chunk_idx}/{len(date_chunks)}: "
                f"{chunk_from.date()} to {chunk_to.date()}",
                chunk=chunk_idx,
                total_chunks=len(date_chunks),
                from_date=chunk_from.date().isoformat(),
                to_date=chunk_to.date().isoformat()
            )
            
            # Build filters
            filters: Dict[str, Any] = {}
            
            if date_field == "fromDate":
                filters["fromDate"] = chunk_from.strftime("%Y-%m-%d")
                filters["toDate"] = chunk_to.strftime("%Y-%m-%d")
            else:
                # updatedAt filters (if API supports them)
                filters["updatedAtFrom"] = chunk_from.isoformat()
                filters["updatedAtTo"] = chunk_to.isoformat()
            
            if modes:
                filters["modes"] = modes
            if bill_type:
                filters["type"] = bill_type
            if customer_id:
                filters["customerId"] = customer_id
            
            # Build request body
            body = {
                "filters": filters,
                "paginator": {
                    "size": 50  # Max page size
                },
                "dataOptions": {
                    "tags": 1,  # Include tags
                    "giftProducts": 1  # Include gift products
                }
            }
            
            try:
                # Fetch all pages for this date chunk
                chunk_bills = self.fetch_paginated("/bill/list", body)
                all_bills.extend(chunk_bills)
                
                logger.info(
                    f"Completed chunk {chunk_idx}: {len(chunk_bills)} bills",
                    chunk=chunk_idx,
                    bills_in_chunk=len(chunk_bills),
                    total_bills=len(all_bills)
                )
            
            except Exception as e:
                logger.error(
                    f"Error fetching bills for chunk {chunk_idx}",
                    error=str(e),
                    chunk=chunk_idx,
                    from_date=chunk_from.date().isoformat(),
                    to_date=chunk_to.date().isoformat()
                )
                # Continue with next chunk instead of failing completely
                continue
        
        logger.info(
            f"Completed bill extraction: {len(all_bills)} total bills",
            total_bills=len(all_bills)
        )
        
        return all_bills

