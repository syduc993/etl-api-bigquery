# Refactoring Summary - Multi-Platform & Multi-Endpoint Support

Tóm tắt về refactoring để hỗ trợ nhiều endpoints và nhiều platforms.

## Vấn đề ban đầu

Pipeline ban đầu chỉ hỗ trợ 3 endpoints của Nhanh API (bills, products, customers) và không có cơ chế để:
- Thêm endpoints mới một cách dễ dàng
- Hỗ trợ nhiều platforms (Facebook, TikTok, 1Offices...)
- Mở rộng mà không phá vỡ code hiện tại

## Giải pháp: Registry Pattern + Plugin Architecture

### 1. Registry Pattern

Tạo `ExtractorRegistry` để quản lý tất cả extractors:

```python
# Đăng ký extractor
registry.register("nhanh", "bills", BillExtractor)

# Sử dụng
extractor = registry.create_instance("nhanh", "bills")
data = extractor.extract()
```

### 2. BaseExtractor Interface

Tất cả extractors implement `BaseExtractor`:

```python
class BaseExtractor(ABC):
    def extract(self, **kwargs) -> List[Dict]
    def get_schema(self) -> Dict
    def get_metadata(self) -> Dict
```

### 3. Platform Structure

Mỗi platform có directory riêng:

```
src/extractors/
├── nhanh/          # Nhanh API extractors
├── facebook/       # Facebook API extractors
├── tiktok/         # TikTok API extractors
└── oneoffices/     # 1Offices API extractors
```

### 4. Config-Driven Endpoints

Endpoints được định nghĩa trong config:

```python
NHANH_ENDPOINTS = {
    "bills": {
        "endpoint": "/bill/list",
        "supports_incremental": True,
        "max_date_range_days": 31
    },
    # Thêm endpoints mới ở đây
}
```

## Những gì đã thay đổi

### File Structure

**Trước:**
```
src/extractors/
├── base.py         # NhanhApiClient
├── bill.py
├── product.py
└── customer.py
```

**Sau:**
```
src/extractors/
├── registry.py     # Registry pattern
├── config.py       # Endpoint configs
├── nhanh/
│   ├── base.py     # NhanhApiClient
│   ├── bill.py
│   ├── product.py
│   ├── customer.py
│   ├── order.py    # Mới
│   └── business.py # Mới
├── facebook/       # Template
├── tiktok/         # Template
└── oneoffices/     # Template
```

### Code Changes

**Trước:**
```python
# Hardcoded extractors
from src.extractors.bill import BillExtractor
extractor = BillExtractor()
bills = extractor.fetch_bills()
```

**Sau:**
```python
# Dynamic từ registry
from src.extractors import registry
extractor = registry.create_instance("nhanh", "bills")
bills = extractor.extract()
```

### Main.py Changes

**Trước:**
```python
# Chỉ hỗ trợ 3 entities cố định
if entity == "bills":
    extractor = BillExtractor()
elif entity == "products":
    extractor = ProductExtractor()
```

**Sau:**
```python
# Hỗ trợ bất kỳ platform/entity nào
extractor = registry.create_instance(platform, entity)
data = extractor.extract(**kwargs)
```

## Lợi ích

### 1. Dễ mở rộng

**Thêm endpoint mới:**
- Chỉ cần tạo extractor class
- Đăng ký trong `__init__.py`
- Tự động available trong pipeline

**Thêm platform mới:**
- Tạo platform directory
- Implement base client
- Tạo extractors
- Đăng ký và sử dụng

### 2. Code Organization

- Mỗi platform có code riêng
- Dễ tìm và maintain
- Clear separation of concerns

### 3. Backward Compatibility

- Code cũ vẫn hoạt động
- Extractors đã được refactor và đăng ký
- Không breaking changes

## Cách sử dụng mới

### Extract từ một platform và entity

```bash
# Nhanh API
python src/main.py --platform nhanh --entity bills
python src/main.py --platform nhanh --entity orders
python src/main.py --platform nhanh --entity depots

# Tất cả entities của Nhanh
python src/main.py --platform nhanh --entity all

# Facebook (khi implement)
python src/main.py --platform facebook --entity orders
```

### List available platforms và entities

```python
from src.extractors import list_available_platforms, list_available_entities

# List platforms
platforms = list_available_platforms()
# ['nhanh', 'facebook', 'tiktok', 'oneoffices']

# List entities của một platform
entities = list_available_entities("nhanh")
# ['bills', 'products', 'customers', 'orders', 'depots', 'users', 'suppliers']
```

## Endpoints đã implement

### Nhanh API
- ✅ bills
- ✅ products
- ✅ customers
- ✅ orders
- ✅ depots
- ✅ users
- ✅ suppliers

### Sẵn sàng để thêm
- 🔄 categories
- 🔄 brands
- 🔄 promotions
- 🔄 accounting_transactions
- 🔄 debts
- 🔄 shipping
- 🔄 và nhiều hơn nữa...

## Platforms đã setup

### Implemented
- ✅ Nhanh API (7 extractors)

### Templates (sẵn sàng implement)
- 🔄 Facebook API
- 🔄 TikTok API
- 🔄 1Offices API

## Migration Guide

### Cho developers

Nếu bạn đã có code sử dụng extractors cũ:

**Old way:**
```python
from src.extractors.bill import BillExtractor
extractor = BillExtractor()
bills = extractor.fetch_bills()
```

**New way (recommended):**
```python
from src.extractors import registry
extractor = registry.create_instance("nhanh", "bills")
bills = extractor.extract()
```

**Old way vẫn hoạt động:**
```python
# Vẫn có thể import trực tiếp
from src.extractors.nhanh.bill import BillExtractor
extractor = BillExtractor()
bills = extractor.fetch_bills()  # Hoặc extractor.extract()
```

## Next Steps

1. **Thêm các endpoints Nhanh còn lại**:
   - Categories, Brands, Promotions
   - Accounting APIs
   - Shipping APIs
   - etc.

2. **Implement các platforms khác**:
   - Facebook API (khi có credentials và docs)
   - TikTok API (khi có credentials và docs)
   - 1Offices API (khi có credentials và docs)

3. **Tạo generic extractor**:
   - Cho các endpoints đơn giản
   - Giảm code duplication

## Documentation

Xem thêm:
- `docs/EXTENSIBILITY_GUIDE.md` - Hướng dẫn chi tiết cách mở rộng
- `docs/ARCHITECTURE.md` - Kiến trúc chi tiết
- `src/extractors/config.py` - Endpoint configurations

