# Extensibility Guide

Hướng dẫn mở rộng ETL pipeline cho các endpoints và platforms mới.

## Kiến trúc

Pipeline được thiết kế với **Registry Pattern** và **Plugin Architecture** để dễ dàng mở rộng:

1. **BaseExtractor**: Abstract base class cho tất cả extractors
2. **ExtractorRegistry**: Quản lý và đăng ký extractors
3. **Platform-specific clients**: Mỗi platform có base client riêng
4. **Config-driven**: Endpoints được định nghĩa trong config

## Thêm Endpoint mới cho Nhanh API

### Bước 1: Thêm endpoint config

Sửa `src/extractors/config.py`:

```python
NHANH_ENDPOINTS = {
    # ... existing endpoints ...
    "categories": {
        "endpoint": "/product/category",
        "supports_incremental": False,
        "supports_date_range": False,
        "default_page_size": 50,
        "filters": []
    }
}
```

### Bước 2: Tạo extractor class

Tạo file `src/extractors/nhanh/category.py`:

```python
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class CategoryExtractor(BaseExtractor, NhanhApiClient):
    """Extractor cho categories từ Nhanh API."""
    
    def __init__(self):
        BaseExtractor.__init__(self, platform="nhanh", entity="categories")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """Extract categories."""
        body = {"paginator": {"size": 50}}
        categories = self.fetch_paginated("/product/category", body)
        logger.info(f"Fetched {len(categories)} categories")
        return categories
    
    def get_schema(self) -> Dict:
        """Lấy schema của categories entity."""
        return {
            "entity": "categories",
            "platform": "nhanh",
            "fields": [
                {"name": "id", "type": "INT64", "required": True},
                {"name": "name", "type": "STRING", "required": True},
            ]
        }
```

### Bước 3: Đăng ký extractor

Sửa `src/extractors/nhanh/__init__.py`:

```python
from src.extractors.nhanh.category import CategoryExtractor

registry.register(PLATFORM, "categories", CategoryExtractor)
```

### Bước 4: Sử dụng

```bash
# Extract categories
python src/main.py --platform nhanh --entity categories

# Extract tất cả entities của Nhanh
python src/main.py --platform nhanh --entity all
```

## Thêm Platform mới (Facebook, TikTok, 1Offices...)

### Bước 1: Tạo platform directory

```bash
mkdir -p src/extractors/facebook
```

### Bước 2: Tạo base client

Tạo `src/extractors/facebook/base.py`:

```python
"""
Base client cho Facebook API.
"""
from typing import Dict, Any, List
from src.utils.logging import get_logger

logger = get_logger(__name__)


class FacebookApiClient:
    """Base client cho Facebook API."""
    
    def __init__(self):
        # Load credentials từ Secret Manager
        # Setup authentication
        # Setup rate limiting
        pass
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Thực hiện Facebook API request."""
        # Implement Facebook API call logic
        pass
    
    def fetch_paginated(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Lấy tất cả pages từ Facebook API."""
        # Implement Facebook pagination logic
        pass
```

### Bước 3: Tạo extractors

Tạo `src/extractors/facebook/order.py`:

```python
from typing import Dict, Any, List
from src.extractors.registry import BaseExtractor
from src.extractors.facebook.base import FacebookApiClient
from src.utils.logging import get_logger

logger = get_logger(__name__)


class FacebookOrderExtractor(BaseExtractor, FacebookApiClient):
    """Extractor cho orders từ Facebook."""
    
    def __init__(self):
        BaseExtractor.__init__(self, platform="facebook", entity="orders")
        FacebookApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """Extract orders từ Facebook."""
        # Implement Facebook order extraction
        pass
    
    def get_schema(self) -> Dict:
        """Lấy schema của Facebook orders entity."""
        return {
            "entity": "orders",
            "platform": "facebook",
            "fields": [...]
        }
```

### Bước 4: Đăng ký platform

Tạo `src/extractors/facebook/__init__.py`:

```python
from src.extractors.registry import registry, BaseExtractor
from src.extractors.facebook.base import FacebookApiClient
from src.extractors.facebook.order import FacebookOrderExtractor

PLATFORM = "facebook"

# Đăng ký extractors
registry.register(PLATFORM, "orders", FacebookOrderExtractor)
```

### Bước 5: Thêm config

Sửa `src/extractors/config.py`:

```python
PLATFORMS = {
    # ... existing platforms ...
    "facebook": {
        "name": "Facebook",
        "base_url": "https://graph.facebook.com/v18.0",
        "rate_limit": 200,  # Facebook rate limit
        "rate_window": 60,
        "endpoints": {
            "orders": {
                "endpoint": "/orders",
                "supports_incremental": True,
                "default_page_size": 100
            }
        }
    }
}
```

### Bước 6: Import trong main module

Sửa `src/extractors/__init__.py`:

```python
# Import Facebook extractors để auto-register
from src.extractors import facebook
```

## Cấu trúc thư mục mới

```
src/extractors/
├── __init__.py              # Main module với registry
├── registry.py              # Registry pattern
├── config.py                # Endpoint và platform configs
├── nhanh/                   # Nhanh API extractors
│   ├── __init__.py          # Đăng ký Nhanh extractors
│   ├── base.py              # NhanhApiClient
│   ├── bill.py
│   ├── product.py
│   ├── customer.py
│   ├── order.py
│   ├── business.py
│   └── ...                  # Thêm extractors mới ở đây
├── facebook/                 # Facebook API extractors
│   ├── __init__.py
│   ├── base.py
│   └── order.py
├── tiktok/                   # TikTok API extractors
│   ├── __init__.py
│   ├── base.py
│   └── order.py
└── oneoffices/               # 1Offices API extractors
    ├── __init__.py
    ├── base.py
    └── order.py
```

## Lợi ích của kiến trúc mới

### 1. Dễ mở rộng
- Thêm endpoint mới: Chỉ cần tạo extractor class và đăng ký
- Thêm platform mới: Tạo platform directory và implement base client

### 2. Tách biệt concerns
- Mỗi platform có code riêng
- Không ảnh hưởng đến platforms khác khi thay đổi

### 3. Config-driven
- Endpoints được định nghĩa trong config
- Dễ dàng thay đổi mà không cần sửa code

### 4. Type safety
- BaseExtractor đảm bảo interface nhất quán
- Dễ dàng test và maintain

## Ví dụ: Thêm endpoint "promotions"

### 1. Thêm config

```python
# src/extractors/config.py
NHANH_ENDPOINTS["promotions"] = {
    "endpoint": "/promotion/batch",
    "supports_incremental": False,
    "default_page_size": 50
}
```

### 2. Tạo extractor

```python
# src/extractors/nhanh/promotion.py
class PromotionExtractor(BaseExtractor, NhanhApiClient):
    def __init__(self):
        BaseExtractor.__init__(self, platform="nhanh", entity="promotions")
        NhanhApiClient.__init__(self)
    
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        body = {"paginator": {"size": 50}}
        return self.fetch_paginated("/promotion/batch", body)
    
    def get_schema(self) -> Dict:
        return {"entity": "promotions", "platform": "nhanh", "fields": [...]}
```

### 3. Đăng ký

```python
# src/extractors/nhanh/__init__.py
from src.extractors.nhanh.promotion import PromotionExtractor
registry.register(PLATFORM, "promotions", PromotionExtractor)
```

### 4. Sử dụng

```bash
python src/main.py --platform nhanh --entity promotions
```

## Best Practices

### 1. Naming Convention
- Platform: lowercase (nhanh, facebook, tiktok)
- Entity: lowercase, plural (bills, products, orders)
- Class: PascalCase (BillExtractor, FacebookOrderExtractor)

### 2. Error Handling
- Luôn log errors với platform và entity context
- Continue với entities khác nếu một entity fail
- Raise exceptions chỉ khi critical

### 3. Schema Definition
- Định nghĩa schema trong `get_schema()` method
- Sử dụng schema này cho BigQuery table creation

### 4. Testing
- Tạo unit tests cho mỗi extractor
- Test với mock data
- Test error scenarios

## Migration từ code cũ

Code cũ vẫn hoạt động vì:
- `BillExtractor`, `ProductExtractor`, `CustomerExtractor` đã được refactor
- Chúng vẫn kế thừa từ `NhanhApiClient` và implement `BaseExtractor`
- Đã được đăng ký trong registry

## Next Steps

1. **Thêm các endpoints còn lại của Nhanh**:
   - Accounting APIs
   - Promotion APIs
   - Shipping APIs
   - etc.

2. **Implement các platforms khác**:
   - Facebook API
   - TikTok API
   - 1Offices API

3. **Tạo generic extractor**:
   - Có thể tạo generic extractor dựa trên config
   - Giảm code duplication cho simple endpoints

