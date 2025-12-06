"""
Configuration cho extractors - định nghĩa endpoints và entities.
File này cho phép dễ dàng thêm endpoints mới mà không cần thay đổi code nhiều.
"""
from typing import Dict, List, Any

# Cấu hình các endpoints của Nhanh API
NHANH_ENDPOINTS = {
    "bills": {
        "endpoint": "/bill/list",
        "supports_incremental": True,
        "supports_date_range": True,
        "max_date_range_days": 31,
        "default_page_size": 50,
        "filters": ["modes", "type", "customerId", "fromDate", "toDate", "updatedAtFrom", "updatedAtTo"]
    },
    "products": {
        "endpoint": "/product/list",
        "supports_incremental": True,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": ["name", "updatedAtFrom", "updatedAtTo"]
    },
    "customers": {
        "endpoint": "/customer/list",
        "supports_incremental": True,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": ["name", "mobile", "updatedAtFrom", "updatedAtTo"]
    },
    "orders": {
        "endpoint": "/order/list",
        "supports_incremental": True,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": ["status", "source", "updatedAtFrom", "updatedAtTo"]
    },
    "depots": {
        "endpoint": "/business/depot",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "users": {
        "endpoint": "/business/user",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "suppliers": {
        "endpoint": "/business/supplier",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "categories": {
        "endpoint": "/product/category",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "brands": {
        "endpoint": "/product/brand",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "promotions": {
        "endpoint": "/promotion/batch",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    },
    "accounting_transactions": {
        "endpoint": "/accounting/transaction",
        "supports_incremental": True,
        "supports_date_range": True,
        "max_date_range_days": 31,
        "default_page_size": 50,
        "filters": ["fromDate", "toDate", "updatedAtFrom", "updatedAtTo"]
    },
    "debts": {
        "endpoint": "/accounting/debts",
        "supports_incremental": True,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": ["updatedAtFrom", "updatedAtTo"]
    }
}

# Cấu hình các platforms
PLATFORMS = {
    "nhanh": {
        "name": "Nhanh.vn",
        "base_url": "https://pos.open.nhanh.vn/v3.0",
        "rate_limit": 150,
        "rate_window": 30,
        "endpoints": NHANH_ENDPOINTS
    },
    "facebook": {
        "name": "Facebook",
        "base_url": None,  # TODO: Add Facebook API base URL
        "rate_limit": None,  # TODO: Add Facebook rate limit
        "rate_window": None,
        "endpoints": {}  # TODO: Add Facebook endpoints
    },
    "tiktok": {
        "name": "TikTok",
        "base_url": None,  # TODO: Add TikTok API base URL
        "rate_limit": None,  # TODO: Add TikTok rate limit
        "rate_window": None,
        "endpoints": {}  # TODO: Add TikTok endpoints
    },
    "oneoffices": {
        "name": "1Offices",
        "base_url": None,  # TODO: Add 1Offices API base URL
        "rate_limit": None,  # TODO: Add 1Offices rate limit
        "rate_window": None,
        "endpoints": {}  # TODO: Add 1Offices endpoints
    }
}


def get_endpoint_config(platform: str, entity: str) -> Dict[str, Any]:
    """
    Lấy cấu hình endpoint cho platform và entity.
    
    Args:
        platform: Tên platform (ví dụ: 'nhanh')
        entity: Tên entity (ví dụ: 'bills')
        
    Returns:
        Dict[str, Any]: Endpoint configuration hoặc empty dict nếu không tìm thấy
    """
    return PLATFORMS.get(platform, {}).get("endpoints", {}).get(entity, {})


def list_available_entities(platform: str) -> List[str]:
    """
    List tất cả entities available cho một platform.
    
    Args:
        platform: Tên platform
        
    Returns:
        List[str]: Danh sách entities
    """
    return list(PLATFORMS.get(platform, {}).get("endpoints", {}).keys())


def list_available_platforms() -> List[str]:
    """
    List tất cả platforms available.
    
    Returns:
        List[str]: Danh sách platforms
    """
    return list(PLATFORMS.keys())

