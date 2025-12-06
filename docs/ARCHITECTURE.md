# Architecture Overview

Tổng quan về kiến trúc ETL pipeline với khả năng mở rộng cho nhiều platforms và endpoints.

## Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL Pipeline                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Nhanh API  │  │  Facebook    │  │    TikTok    │     │
│  │              │  │     API      │  │     API      │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                 │                  │              │
│         └─────────────────┼──────────────────┘              │
│                           │                                 │
│                  ┌────────▼────────┐                        │
│                  │ Extractor       │                        │
│                  │ Registry        │                        │
│                  │ (BaseExtractor) │                        │
│                  └────────┬────────┘                        │
│                           │                                 │
│                  ┌────────▼────────┐                        │
│                  │  GCS Loader     │                        │
│                  │  (Bronze Layer) │                        │
│                  └────────┬────────┘                        │
│                           │                                 │
│                  ┌────────▼────────┐                        │
│                  │  BigQuery       │                        │
│                  │  (Silver Layer) │                        │
│                  └────────┬────────┘                        │
│                           │                                 │
│                  ┌────────▼────────┐                        │
│                  │  BigQuery       │                        │
│                  │  (Gold Layer)  │                        │
│                  └─────────────────┘                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Registry Pattern

### BaseExtractor (Abstract Interface)

```python
class BaseExtractor(ABC):
    def extract(self, **kwargs) -> List[Dict]
    def get_schema(self) -> Dict
    def get_metadata(self) -> Dict
```

Tất cả extractors phải implement interface này.

### ExtractorRegistry

Registry quản lý extractors theo cấu trúc:
```
{
    "nhanh": {
        "bills": BillExtractor,
        "products": ProductExtractor,
        "customers": CustomerExtractor,
        "orders": OrderExtractor,
        ...
    },
    "facebook": {
        "orders": FacebookOrderExtractor,
        ...
    },
    "tiktok": {
        "orders": TikTokOrderExtractor,
        ...
    }
}
```

## Platform Structure

Mỗi platform có cấu trúc riêng:

```
src/extractors/
├── registry.py              # Registry pattern
├── config.py                # Endpoint configs
├── nhanh/
│   ├── __init__.py          # Đăng ký extractors
│   ├── base.py              # NhanhApiClient
│   ├── bill.py
│   ├── product.py
│   └── ...
├── facebook/
│   ├── __init__.py
│   ├── base.py              # FacebookApiClient
│   └── ...
└── ...
```

## Data Flow

### Bronze Layer (Multi-Platform)

```
Platform APIs → Extractors → GCS (partitioned by platform/entity)
```

**GCS Structure:**
```
gs://bucket-bronze/
├── nhanh/
│   ├── bills/year=2024/month=01/...
│   ├── products/year=2024/month=01/...
│   └── ...
├── facebook/
│   ├── orders/year=2024/month=01/...
│   └── ...
└── tiktok/
    └── ...
```

### Silver Layer

```
GCS Bronze → BigQuery External Tables → Transformations → Silver Tables
```

**BigQuery Structure:**
```
bronze/
├── nhanh_bills_raw
├── nhanh_products_raw
├── facebook_orders_raw
└── ...

silver/
├── bills (from nhanh)
├── products (from nhanh)
├── orders (from nhanh, facebook, tiktok - unified)
└── ...
```

### Gold Layer

```
Silver Tables → Aggregations → Gold Tables
```

**Gold Tables:**
- Unified views cho cross-platform analytics
- Platform-specific aggregates
- Business metrics

## Extensibility Points

### 1. Thêm Endpoint mới (Nhanh API)

1. Thêm config trong `config.py`
2. Tạo extractor class
3. Đăng ký trong `nhanh/__init__.py`
4. Done! Tự động available trong pipeline

### 2. Thêm Platform mới

1. Tạo platform directory
2. Implement base client
3. Tạo extractors
4. Đăng ký trong `__init__.py`
5. Thêm config trong `config.py`
6. Done! Platform mới sẵn sàng sử dụng

### 3. Custom Transformations

- Silver: Thêm SQL trong `sql/silver/`
- Gold: Thêm SQL trong `sql/gold/`
- Python: Thêm transformers trong `src/transformers/`

## Benefits

### 1. Scalability
- Dễ dàng thêm endpoints mà không ảnh hưởng code hiện tại
- Mỗi platform độc lập

### 2. Maintainability
- Code được tổ chức theo platform
- Dễ tìm và sửa lỗi
- Clear separation of concerns

### 3. Testability
- Mỗi extractor có thể test độc lập
- Mock dễ dàng với BaseExtractor interface

### 4. Flexibility
- Config-driven endpoints
- Runtime discovery của extractors
- Dynamic platform/entity selection

## Example: Adding New Endpoint

```python
# 1. Add config
NHANH_ENDPOINTS["categories"] = {
    "endpoint": "/product/category",
    "supports_incremental": False
}

# 2. Create extractor
class CategoryExtractor(BaseExtractor, NhanhApiClient):
    def extract(self, **kwargs):
        return self.fetch_paginated("/product/category", {...})

# 3. Register
registry.register("nhanh", "categories", CategoryExtractor)

# 4. Use
python src/main.py --platform nhanh --entity categories
```

## Example: Adding New Platform

```python
# 1. Create base client
class FacebookApiClient:
    def _make_request(self, endpoint, params):
        # Facebook API logic
        pass

# 2. Create extractor
class FacebookOrderExtractor(BaseExtractor, FacebookApiClient):
    def extract(self, **kwargs):
        # Facebook extraction logic
        pass

# 3. Register
registry.register("facebook", "orders", FacebookOrderExtractor)

# 4. Use
python src/main.py --platform facebook --entity orders
```

## Current Status

### Implemented
- ✅ Registry pattern
- ✅ BaseExtractor interface
- ✅ Nhanh API extractors (bills, products, customers, orders, depots, users, suppliers)
- ✅ Config-driven endpoints
- ✅ Multi-platform structure (templates cho Facebook, TikTok, 1Offices)

### Ready for Extension
- 🔄 Thêm endpoints Nhanh còn lại (categories, brands, promotions, accounting, etc.)
- 🔄 Implement Facebook API
- 🔄 Implement TikTok API
- 🔄 Implement 1Offices API

## Migration Notes

Code cũ vẫn hoạt động vì:
- Extractors đã được refactor và đăng ký trong registry
- Main.py và orchestrator đã được update để dùng registry
- Backward compatible với cách gọi cũ

