"""
Pydantic validators cho schema validation.
File này cung cấp các Pydantic models để validate data từ Nhanh API
trước khi load vào Bronze/Silver layer.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Kết quả validation cho một batch records."""
    total_records: int
    valid_records: int
    invalid_records: int
    errors: List[Dict[str, Any]]
    
    @property
    def success_rate(self) -> float:
        """Tính tỷ lệ thành công."""
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records


class BillRecord(BaseModel):
    """
    Pydantic model cho Bill record từ Nhanh API.
    
    Validates các trường bắt buộc và data types.
    """
    id: int = Field(..., description="ID của hóa đơn")
    date: Optional[str] = Field(None, description="Ngày tạo hóa đơn")
    type: Optional[int] = Field(None, description="Loại hóa đơn")
    mode: Optional[int] = Field(None, description="Mode của hóa đơn")
    depotId: Optional[int] = Field(None, description="ID của kho")
    orderId: Optional[int] = Field(None, description="ID của đơn hàng liên quan")
    
    # Nested objects - optional
    customer: Optional[Dict[str, Any]] = Field(None, description="Thông tin khách hàng")
    products: Optional[List[Dict[str, Any]]] = Field(None, description="Danh sách sản phẩm")
    payment: Optional[Dict[str, Any]] = Field(None, description="Thông tin thanh toán")
    
    model_config = {
        "extra": "allow"  # Allow extra fields from API
    }


class ProductRecord(BaseModel):
    """
    Pydantic model cho Product record từ Nhanh API.
    
    Validates các trường bắt buộc và data types.
    """
    id: int = Field(..., alias="idNhanh", description="ID sản phẩm trên Nhanh")
    name: Optional[str] = Field(None, description="Tên sản phẩm")
    code: Optional[str] = Field(None, description="Mã sản phẩm")
    barcode: Optional[str] = Field(None, description="Barcode")
    
    # Pricing
    price: Optional[float] = Field(None, description="Giá bán")
    importPrice: Optional[float] = Field(None, description="Giá nhập")
    
    # Category
    categoryId: Optional[int] = Field(None, description="ID danh mục")
    brandId: Optional[int] = Field(None, description="ID thương hiệu")
    
    # Status
    status: Optional[int] = Field(None, description="Trạng thái sản phẩm")
    
    model_config = {
        "extra": "allow",
        "populate_by_name": True  # Allow both idNhanh and id
    }
    
    @field_validator('id', mode='before')
    @classmethod
    def coerce_id(cls, v):
        """Convert string ID to int if needed."""
        if isinstance(v, str):
            return int(v)
        return v


class CustomerRecord(BaseModel):
    """
    Pydantic model cho Customer record từ Nhanh API.
    
    Validates các trường bắt buộc và data types.
    """
    id: int = Field(..., description="ID khách hàng")
    name: Optional[str] = Field(None, description="Tên khách hàng")
    mobile: Optional[str] = Field(None, description="Số điện thoại")
    email: Optional[str] = Field(None, description="Email")
    address: Optional[str] = Field(None, description="Địa chỉ")
    
    # Location
    cityId: Optional[int] = Field(None, description="ID thành phố")
    districtId: Optional[int] = Field(None, description="ID quận/huyện")
    
    # Stats
    totalMoney: Optional[float] = Field(None, description="Tổng tiền đã mua")
    orderCount: Optional[int] = Field(None, description="Số đơn hàng")
    
    model_config = {
        "extra": "allow"
    }


def validate_records(
    records: List[Dict[str, Any]],
    model_class: type[BaseModel]
) -> ValidationResult:
    """
    Validate danh sách records với Pydantic model.
    
    Args:
        records: Danh sách records cần validate
        model_class: Pydantic model class để validate
        
    Returns:
        ValidationResult với thông tin validation
    """
    valid_count = 0
    invalid_count = 0
    errors = []
    
    for idx, record in enumerate(records):
        try:
            model_class.model_validate(record)
            valid_count += 1
        except Exception as e:
            invalid_count += 1
            errors.append({
                'index': idx,
                'record_id': record.get('id', 'unknown'),
                'error': str(e)
            })
    
    return ValidationResult(
        total_records=len(records),
        valid_records=valid_count,
        invalid_records=invalid_count,
        errors=errors
    )
