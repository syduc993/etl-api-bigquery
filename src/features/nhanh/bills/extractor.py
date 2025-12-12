"""
Extractor cho hóa đơn (bills) từ Nhanh API.

Features:
- Tự động chia date range thành các chunks 31 ngày (do API giới hạn)
- Hỗ trợ incremental extraction dựa trên updatedAt
- Hỗ trợ các filters: modes, type, customerId, fromDate/toDate
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from src.shared.nhanh import NhanhApiClient
from src.shared.logging import get_logger
from .types import BillSchema

logger = get_logger(__name__)


class BillExtractor:
    """
    Extractor cho hóa đơn/invoices từ Nhanh API.
    
    Tự động xử lý giới hạn 31 ngày của API bằng cách
    chia date range thành các chunks nhỏ hơn nếu cần.
    """
    
    def __init__(self):
        """Khởi tạo BillExtractor với Nhanh API client."""
        self.client = NhanhApiClient()
        self.platform = "nhanh"
        self.entity = "bills"
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract bills - wrapper cho fetch_bills.
        
        Args:
            **kwargs: Các tham số cho fetch_bills
            
        Returns:
            List[Dict[str, Any]]: Danh sách bills
        """
        return self.fetch_bills(**kwargs)
    
    def get_schema(self) -> Dict:
        """Lấy schema của bills entity."""
        return BillSchema.to_dict()
    
    def fetch_bills(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        modes: Optional[List[int]] = None,
        bill_type: Optional[int] = None,
        customer_id: Optional[int] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None,
        process_by_day: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Lấy danh sách hóa đơn với xử lý giới hạn 31 ngày.
        
        Args:
            from_date: Ngày bắt đầu cho filter bill date
            to_date: Ngày kết thúc cho filter bill date
            modes: Danh sách mode IDs (ví dụ: [2] cho bán lẻ)
            bill_type: Loại hóa đơn (1 = nhập kho, 2 = xuất kho)
            customer_id: Lọc theo ID khách hàng
            updated_at_from: Ngày bắt đầu cho filter updatedAt (cho incremental)
            updated_at_to: Ngày kết thúc cho filter updatedAt (cho incremental)
            process_by_day: Nếu True, xử lý từng ngày riêng biệt
            
        Returns:
            List[Dict[str, Any]]: Danh sách các hóa đơn
        """
        all_bills = []
        
        # Determine date range
        if updated_at_from and updated_at_to:
            if process_by_day:
                date_chunks = self.client.split_date_range_by_day(updated_at_from, updated_at_to)
            else:
                date_chunks = self.client.split_date_range(updated_at_from, updated_at_to)
            date_field = "updatedAtFrom"
            date_to_field = "updatedAtTo"
        elif from_date and to_date:
            if process_by_day:
                date_chunks = self.client.split_date_range_by_day(from_date, to_date)
            else:
                date_chunks = self.client.split_date_range(from_date, to_date)
            date_field = "fromDate"
            date_to_field = "toDate"
        else:
            # Default: last 31 days
            to_date = datetime.now()
            from_date = to_date - timedelta(days=30)
            if process_by_day:
                date_chunks = self.client.split_date_range_by_day(from_date, to_date)
            else:
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
                total_chunks=len(date_chunks)
            )
            
            # Build filters
            filters: Dict[str, Any] = {}
            
            if date_field == "fromDate":
                if process_by_day:
                    day_str = chunk_from.strftime("%Y-%m-%d")
                    filters["fromDate"] = day_str
                    filters["toDate"] = day_str
                else:
                    filters["fromDate"] = chunk_from.strftime("%Y-%m-%d")
                    filters["toDate"] = chunk_to.strftime("%Y-%m-%d")
            else:
                filters["updatedAtFrom"] = chunk_from.isoformat()
                filters["updatedAtTo"] = chunk_to.isoformat()
            
            if modes:
                filters["modes"] = modes
            if bill_type:
                filters["type"] = bill_type
            if customer_id:
                filters["customerId"] = customer_id
            
            body = {
                "filters": filters,
                "paginator": {"size": 50},
                "dataOptions": {
                    "tags": 1,
                    "giftProducts": 1
                }
            }
            
            try:
                chunk_bills = self.client.fetch_paginated("/bill/list", body)
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
                    chunk=chunk_idx
                )
                continue
        
        logger.info(
            f"Completed bill extraction: {len(all_bills)} total bills",
            total_bills=len(all_bills)
        )
        
        return all_bills
    
    def extract_with_products(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        modes: Optional[List[int]] = None,
        bill_type: Optional[int] = None,
        customer_id: Optional[int] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None,
        process_by_day: bool = False
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Lấy danh sách hóa đơn và tách products ra thành danh sách riêng.
        
        Returns:
            tuple: (bills_list, products_list)
        """
        all_bills = self.fetch_bills(
            from_date=from_date,
            to_date=to_date,
            modes=modes,
            bill_type=bill_type,
            customer_id=customer_id,
            updated_at_from=updated_at_from,
            updated_at_to=updated_at_to,
            process_by_day=process_by_day
        )
        
        bills_without_products = []
        all_products = []
        
        for bill in all_bills:
            bill_copy = bill.copy()
            products_data = bill_copy.pop('products', [])
            
            bill_copy['bill_id'] = bill.get('id', f"bill_{len(bills_without_products)}")
            bills_without_products.append(bill_copy)
            
            if products_data:
                if isinstance(products_data, dict):
                    for product_id, product_data in products_data.items():
                        if isinstance(product_data, dict):
                            product_record = product_data.copy()
                        else:
                            product_record = {'data': product_data}
                        product_record['bill_id'] = bill_copy['bill_id']
                        product_record['product_id'] = product_id
                        all_products.append(product_record)
                elif isinstance(products_data, list):
                    for idx, product_data in enumerate(products_data):
                        if isinstance(product_data, dict):
                            product_record = product_data.copy()
                        else:
                            product_record = {'data': product_data}
                        product_record['bill_id'] = bill_copy['bill_id']
                        if 'product_id' not in product_record:
                            product_record['product_id'] = product_record.get('id', idx)
                        
                        # Enforce types to avoid Parquet schema mismatch (INT64 vs DOUBLE)
                        # BigQuery expects DOUBLE for these fields
                        try:
                            if 'discount' in product_record and product_record['discount'] is not None:
                                product_record['discount'] = float(product_record['discount'])
                            
                            if 'quantity' in product_record and product_record['quantity'] is not None:
                                product_record['quantity'] = float(product_record['quantity'])
                                
                            if 'price' in product_record and product_record['price'] is not None:
                                product_record['price'] = float(product_record['price'])
                            
                            if 'vat' in product_record and isinstance(product_record['vat'], dict):
                                if 'amount' in product_record['vat'] and product_record['vat']['amount'] is not None:
                                    # Wait, the structure is vat.amount. 
                                    # The original code just leaves it as is.
                                    pass
                                
                            # Flatten/Normalize VAT (if needed, but here we just ensure types in place if it's used later)
                            # Actually, looking at query_flatten.sql:
                            # 78:     vat.percent AS vat_percent,
                            # 79:     vat.amount AS vat_amount,
                            # So we should probably allow the dict to stay, but ensure values inside are typed.
                            if 'vat' in product_record and isinstance(product_record['vat'], dict):
                                if 'amount' in product_record['vat'] and product_record['vat']['amount'] is not None:
                                     product_record['vat']['amount'] = float(product_record['vat']['amount'])
                                if 'percent' in product_record['vat'] and product_record['vat']['percent'] is not None:
                                     product_record['vat']['percent'] = int(product_record['vat']['percent'])

                            if 'amount' in product_record and product_record['amount'] is not None:
                                product_record['amount'] = float(product_record['amount'])

                        except (ValueError, TypeError) as e:
                            # Log warning but keep going with original value? Or set to 0? 
                            # Safe to keep original, but logging helps.
                            # logger.warning(f"Failed to cast product fields: {e}")
                            pass

                        all_products.append(product_record)
        
        logger.info(
            f"Separated bills and products",
            bills_count=len(bills_without_products),
            products_count=len(all_products)
        )
        
        return bills_without_products, all_products
