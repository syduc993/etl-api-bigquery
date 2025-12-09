"""
Type definitions và schema cho Bills feature.
"""
from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class BillSchema:
    """Schema definition cho bills entity."""
    
    entity: str = "bills"
    platform: str = "nhanh"
    
    @staticmethod
    def get_fields() -> List[Dict[str, Any]]:
        """Lấy danh sách fields của bills."""
        return [
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
    
    @classmethod
    def to_dict(cls) -> Dict:
        """Convert schema to dictionary."""
        return {
            "entity": cls.entity,
            "platform": cls.platform,
            "fields": cls.get_fields()
        }
