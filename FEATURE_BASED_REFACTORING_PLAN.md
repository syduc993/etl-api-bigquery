# Feature-Based Architecture Refactoring Plan

## 📋 Tổng quan

Tài liệu này mô tả kế hoạch refactor dự án ETL Pipeline theo hướng **Feature-Based Architecture** (tương tự React best practices), giúp:
- ✅ Dễ dàng mở rộng khi thêm features mới
- ✅ Tách biệt concerns rõ ràng
- ✅ Tăng khả năng maintain và test
- ✅ Team members có thể làm việc độc lập trên từng feature

---

## 🏗️ Cấu trúc hiện tại

```
src/
├── config.py                    # Global config
├── main.py                      # Entry point Bronze
├── orchestrator.py              # Pipeline orchestrator
├── transform_silver.py          # Silver entry point
├── transform_gold.py            # Gold entry point
├── extractors/                  # Extractors by platform
│   ├── registry.py
│   ├── config.py
│   ├── nhanh/
│   ├── facebook/
│   ├── tiktok/
│   └── oneoffices/
├── loaders/                     # GCS loaders
├── transformers/                # Bronze → Silver → Gold
├── quality/                     # Data quality checks
├── monitoring/                  # Metrics
└── utils/                       # Shared utilities
```

### 🔴 Vấn đề với cấu trúc hiện tại

1. **Technical-focused**: Tổ chức theo layer/technology thay vì business domain
2. **Cross-dependencies**: Khó trace code flow của một entity cụ thể
3. **Scaling issues**: Khi thêm entity mới, phải sửa nhiều folders khác nhau
4. **Testing complexity**: Tests không gắn liền với feature
5. **Transformers coupling**: `bronze_to_silver.py` và `silver_to_gold.py` chứa logic của nhiều entities

---

## 🎯 Cấu trúc đề xuất: Feature-Based Architecture

> [!IMPORTANT]
> **Platform Isolation**: Mỗi platform (Nhanh, Facebook, TikTok) có pipeline riêng biệt.
> Facebook lỗi sẽ **KHÔNG** ảnh hưởng đến Nhanh pipeline.

```
src/
├── core/                           # 🔧 Core infrastructure (shared)
│   ├── __init__.py
│   ├── config.py                   # Global settings
│   ├── exceptions.py               # Custom exceptions
│   ├── logging.py                  # Logging setup
│   └── interfaces/                 # Abstract base classes
│       ├── __init__.py
│       ├── extractor.py            # IExtractor interface
│       ├── transformer.py          # ITransformer interface
│       └── loader.py               # ILoader interface
│
├── shared/                         # 📦 Shared utilities ONLY (no business logic)
│   ├── __init__.py
│   ├── loaders/                    # Generic data loaders
│   │   ├── gcs_loader.py
│   │   └── bigquery_loader.py
│   ├── quality/                    # Generic quality framework
│   │   ├── checks.py
│   │   └── validators.py
│   └── monitoring/                 # Generic metrics
│       └── metrics.py
│
├── platforms/                      # 🌐 ISOLATED Platforms (thay vì features chung)
│   ├── __init__.py
│   │
│   ├── nhanh/                      # � NHANH PLATFORM (hoàn toàn độc lập)
│   │   ├── __init__.py
│   │   ├── client.py               # NhanhApiClient
│   │   ├── config.py               # Nhanh-specific config
│   │   ├── pipeline.py             # 🔥 NHANH PIPELINE RIÊNG
│   │   ├── features/               # Nhanh features
│   │   │   ├── bills/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── extractor.py
│   │   │   │   ├── transformer.py
│   │   │   │   ├── models.py
│   │   │   │   ├── sql/
│   │   │   │   └── tests/          # ✅ Tests CO-LOCATED
│   │   │   │       ├── test_extractor.py
│   │   │   │       ├── test_transformer.py
│   │   │   │       └── fixtures/
│   │   │   ├── products/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── extractor.py
│   │   │   │   ├── transformer.py
│   │   │   │   ├── sql/
│   │   │   │   └── tests/          # ✅ Tests CO-LOCATED
│   │   │   ├── customers/
│   │   │   ├── orders/
│   │   │   └── business/
│   │   └── tests/                  # Nhanh integration tests
│   │       └── test_pipeline.py
│   │
│   ├── facebook/                   # 🔵 FACEBOOK PLATFORM (hoàn toàn độc lập)
│   │   ├── __init__.py
│   │   ├── client.py               # FacebookApiClient
│   │   ├── config.py               # Facebook-specific config
│   │   ├── pipeline.py             # 🔥 FACEBOOK PIPELINE RIÊNG
│   │   ├── features/
│   │   │   ├── ads/
│   │   │   │   ├── extractor.py
│   │   │   │   ├── transformer.py
│   │   │   │   └── tests/
│   │   │   └── insights/
│   │   └── tests/
│   │
│   ├── tiktok/                     # 🎵 TIKTOK PLATFORM (hoàn toàn độc lập)
│   │   ├── __init__.py
│   │   ├── client.py
│   │   ├── config.py
│   │   ├── pipeline.py             # 🔥 TIKTOK PIPELINE RIÊNG
│   │   ├── features/
│   │   └── tests/
│   │
│   └── oneoffices/                 # 🏢 1OFFICES PLATFORM
│       └── ... (same structure)
│
├── orchestrator/                   # 🎛️ Top-level orchestration
│   ├── __init__.py
│   ├── multi_platform.py           # Chạy nhiều platforms (với error isolation)
│   └── scheduler.py                # Scheduling logic
│
├── registry/                       # 📝 Platform & Feature registry
│   ├── __init__.py
│   └── platform_registry.py
│
└── main.py                         # 🚀 Application entry point

tests/                              # E2E tests only (cross-platform)
├── e2e/
└── fixtures/
```

---

## 📐 Chi tiết thiết kế từng component

### 1. Core Module (`src/core/`)

Chứa các abstract classes và interfaces, đảm bảo tất cả features tuân theo contract thống nhất.

```python
# src/core/interfaces/extractor.py
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any

class IExtractor(ABC):
    """Interface cho tất cả extractors."""
    
    @property
    @abstractmethod
    def entity_name(self) -> str:
        """Tên entity (bills, products, etc.)"""
        pass
    
    @property
    @abstractmethod
    def platform(self) -> str:
        """Platform name (nhanh, facebook, etc.)"""
        pass
    
    @abstractmethod
    def extract(self, **kwargs) -> Generator[Dict[str, Any], None, None]:
        """Extract data từ source."""
        pass
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate extracted data."""
        pass
```

```python
# src/core/interfaces/transformer.py
from abc import ABC, abstractmethod

class ITransformer(ABC):
    """Interface cho tất cả transformers."""
    
    @abstractmethod
    def transform_to_silver(self, bronze_path: str) -> str:
        """Transform Bronze → Silver."""
        pass
    
    @abstractmethod
    def transform_to_gold(self, silver_table: str) -> str:
        """Transform Silver → Gold."""
        pass
```

---

### 2. Platform-Based Feature Module

Mỗi **platform** có **pipeline riêng biệt** và các features của nó được đóng gói bên trong.

```python
# src/platforms/nhanh/__init__.py
"""Nhanh Platform - Hoàn toàn độc lập với các platforms khác."""
from .pipeline import NhanhPipeline
from .client import NhanhApiClient

__all__ = ['NhanhPipeline', 'NhanhApiClient']

PLATFORM_CONFIG = {
    'name': 'nhanh',
    'enabled': True,
    'api_base_url': 'https://open.nhanh.vn/api',
    'rate_limit': 150,  # requests per 30 seconds
}
```

```python
# src/platforms/nhanh/pipeline.py
"""🔥 NHANH PIPELINE - Chỉ xử lý Nhanh features, không phụ thuộc platforms khác."""
from typing import List, Optional
from .features.bills import BillExtractor, BillTransformer
from .features.products import ProductExtractor, ProductTransformer
from src.shared.loaders import GCSLoader
from src.shared.monitoring import MetricsCollector
import logging

logger = logging.getLogger(__name__)

class NhanhPipeline:
    """Pipeline hoàn toàn độc lập cho Nhanh platform.
    
    ✅ Facebook lỗi KHÔNG ảnh hưởng đến pipeline này.
    ✅ Có thể chạy độc lập, test độc lập.
    """
    
    def __init__(self):
        self.gcs_loader = GCSLoader()
        self.metrics = MetricsCollector(platform='nhanh')
        
        # Đăng ký tất cả features của Nhanh
        self.features = {
            'bills': (BillExtractor(), BillTransformer()),
            'products': (ProductExtractor(), ProductTransformer()),
            # Thêm features khác ở đây
        }
    
    def run_bronze(self, entities: Optional[List[str]] = None, **kwargs):
        """Chạy Bronze extraction cho Nhanh entities."""
        entities = entities or list(self.features.keys())
        results = {}
        
        for entity in entities:
            try:
                extractor, _ = self.features[entity]
                data = list(extractor.extract(**kwargs))
                gcs_path = self.gcs_loader.upload(data, 'nhanh', entity)
                
                results[entity] = {'status': 'success', 'records': len(data)}
                self.metrics.record_extraction(entity, len(data))
                logger.info(f"✅ Nhanh/{entity}: {len(data)} records")
                
            except Exception as e:
                results[entity] = {'status': 'error', 'error': str(e)}
                logger.error(f"❌ Nhanh/{entity}: {e}")
                # Continue với entities khác, không fail toàn bộ pipeline
        
        return results
    
    def run_silver(self, entities: Optional[List[str]] = None):
        """Chạy Silver transformation cho Nhanh entities."""
        # Similar pattern...
        pass
    
    def run_full(self, entities: Optional[List[str]] = None, **kwargs):
        """Chạy Bronze → Silver → Gold cho Nhanh."""
        bronze_results = self.run_bronze(entities, **kwargs)
        silver_results = self.run_silver(entities)
        # gold_results = self.run_gold(entities)
        return {'bronze': bronze_results, 'silver': silver_results}
```

```python
# src/platforms/nhanh/features/bills/extractor.py
from src.core.interfaces import IExtractor
from src.platforms.nhanh.client import NhanhApiClient  # 👈 Import từ platform
from .models import Bill
from .constants import BILLS_ENDPOINT

class BillExtractor(IExtractor):
    """Extractor cho Nhanh Bills."""
    
    def __init__(self):
        self.client = NhanhApiClient()  # Nhanh-specific client
    
    @property
    def entity_name(self) -> str:
        return "bills"
    
    @property
    def platform(self) -> str:
        return "nhanh"
    
    def extract(self, from_date=None, to_date=None, **kwargs):
        date_ranges = self.client.split_date_range(from_date, to_date)
        for start, end in date_ranges:
            for record in self.client.fetch_paginated(
                BILLS_ENDPOINT,
                body={'fromDate': start, 'toDate': end}
            ):
                yield self._map_to_model(record)
    
    def _map_to_model(self, raw_data: dict) -> Bill:
        return Bill(**raw_data)
```

---

### 3. 🔐 Error Isolation - Platform Independence

> [!CAUTION]
> **Đây là điểm quan trọng nhất của thiết kế mới!**

```python
# src/orchestrator/multi_platform.py
"""Top-level orchestrator với ERROR ISOLATION giữa các platforms."""
import concurrent.futures
from typing import List, Optional, Dict
from src.platforms.nhanh import NhanhPipeline
from src.platforms.facebook import FacebookPipeline
from src.platforms.tiktok import TikTokPipeline
import logging

logger = logging.getLogger(__name__)

class MultiPlatformOrchestrator:
    """Orchestrator đảm bảo ERROR ISOLATION giữa các platforms.
    
    ⚠️ Facebook API lỗi → Chỉ Facebook fail
    ✅ Nhanh API vẫn chạy bình thường
    ✅ TikTok API vẫn chạy bình thường
    """
    
    def __init__(self):
        # Mỗi platform có pipeline riêng biệt
        self.pipelines = {
            'nhanh': NhanhPipeline(),
            'facebook': FacebookPipeline(),
            'tiktok': TikTokPipeline(),
        }
    
    def run_all(self, platforms: Optional[List[str]] = None, parallel: bool = True):
        """Chạy tất cả platforms với error isolation."""
        platforms = platforms or list(self.pipelines.keys())
        results: Dict[str, dict] = {}
        
        if parallel:
            # Chạy song song - mỗi platform trong thread riêng
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self._run_single_platform, name): name
                    for name in platforms
                }
                
                for future in concurrent.futures.as_completed(futures):
                    platform_name = futures[future]
                    try:
                        results[platform_name] = future.result()
                    except Exception as e:
                        # ⚠️ ERROR ISOLATION: Platform này fail
                        # nhưng các platforms khác vẫn chạy
                        results[platform_name] = {
                            'status': 'error',
                            'error': str(e),
                            'isolated': True  # Indicates error was isolated
                        }
                        logger.error(f"❌ Platform {platform_name} failed: {e}")
        else:
            # Chạy tuần tự với try-except cho mỗi platform
            for name in platforms:
                try:
                    results[name] = self._run_single_platform(name)
                except Exception as e:
                    results[name] = {'status': 'error', 'error': str(e)}
                    logger.error(f"❌ Platform {name} failed, continuing...")
                    # Continue với platform tiếp theo!
        
        self._log_summary(results)
        return results
    
    def _run_single_platform(self, platform_name: str):
        """Chạy một platform duy nhất."""
        pipeline = self.pipelines[platform_name]
        return pipeline.run_full()
    
    def _log_summary(self, results: Dict[str, dict]):
        """Log tổng kết."""
        success = [p for p, r in results.items() if r.get('status') != 'error']
        failed = [p for p, r in results.items() if r.get('status') == 'error']
        
        logger.info(f"\n{'='*50}")
        logger.info(f"✅ Successful: {success}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"{'='*50}\n")
```

**Lợi ích của Error Isolation:**

| Scenario | Cấu trúc cũ | Cấu trúc mới |
|----------|-------------|---------------|
| Facebook API down | ❌ Toàn bộ pipeline fail | ✅ Chỉ Facebook fail |
| Nhanh rate limit | ❌ Ảnh hưởng chung | ✅ Chỉ Nhanh retry |
| TikTok auth expired | ❌ Có thể crash others | ✅ Isolated failure |
| Debug 1 platform | ❌ Phải trace nhiều files | ✅ Chỉ check `platforms/nhanh/` |

---

### 4. Platform Registry

```python
# src/registry/platform_registry.py
from typing import Dict, Type
from src.core.interfaces import IPipeline

class PlatformRegistry:
    """Registry cho các platforms."""
    
    _pipelines: Dict[str, Type[IPipeline]] = {}
    
    @classmethod
    def register(cls, name: str, pipeline_class: Type[IPipeline]):
        """Đăng ký một platform."""
        cls._pipelines[name] = pipeline_class
    
    @classmethod
    def get(cls, name: str) -> IPipeline:
        """Lấy pipeline instance cho platform."""
        return cls._pipelines[name]()
    
    @classmethod
    def list_platforms(cls):
        """Danh sách tất cả platforms đã đăng ký."""
        return list(cls._pipelines.keys())

# Auto-registration
def auto_discover_platforms():
    """Tự động discover platforms từ src/platforms/"""
    from importlib import import_module
    from pathlib import Path
    
    platforms_dir = Path(__file__).parent.parent / 'platforms'
    for platform_path in platforms_dir.iterdir():
        if platform_path.is_dir() and not platform_path.name.startswith('_'):
            try:
                module = import_module(f'src.platforms.{platform_path.name}')
                if hasattr(module, 'PLATFORM_CONFIG'):
                    config = module.PLATFORM_CONFIG
                    pipeline_class = getattr(module, f'{config["name"].title()}Pipeline')
                    PlatformRegistry.register(config['name'], pipeline_class)
            except Exception as e:
                # Log but don't fail - platform có thể chưa implement đầy đủ
                print(f"Warning: Could not load platform {platform_path.name}: {e}")
```

---

## 📋 Kế hoạch thực hiện (Migration Steps)

### Phase 1: Setup Core & Shared (Tuần 1)

| # | Task | Files |
|---|------|-------|
| 1.1 | Tạo `src/core/` structure | `core/__init__.py`, `core/config.py`, `core/exceptions.py`, `core/logging.py` |
| 1.2 | Tạo interfaces | `core/interfaces/extractor.py`, `core/interfaces/transformer.py`, `core/interfaces/pipeline.py` |
| 1.3 | Migrate utils → core | Move từ `utils/` sang `core/` |
| 1.4 | Tạo `src/shared/` | `shared/loaders/`, `shared/quality/`, `shared/monitoring/` |
| 1.5 | Migrate loaders | `loaders/gcs_loader.py` → `shared/loaders/` |
| 1.6 | Migrate quality | `quality/` → `shared/quality/` |

### Phase 2: Migrate Nhanh Platform (Tuần 2-3)

| # | Task | Description |
|---|------|-------------|
| 2.1 | Tạo `platforms/nhanh/` structure | Skeleton với `__init__.py`, `client.py`, `config.py`, `pipeline.py` |
| 2.2 | Migrate NhanhApiClient | `extractors/nhanh/base.py` → `platforms/nhanh/client.py` |
| 2.3 | Tạo NhanhPipeline | `platforms/nhanh/pipeline.py` với error handling |
| 2.4 | Migrate Bills feature | `extractors/nhanh/bill.py` → `platforms/nhanh/features/bills/` |
| 2.5 | Move Bills SQL | `sql/*/bills.sql` → `platforms/nhanh/features/bills/sql/` |
| 2.6 | Co-locate Bills tests | → `platforms/nhanh/features/bills/tests/` |
| 2.7 | Migrate remaining features | products, customers, orders, business |
| 2.8 | Verify Nhanh pipeline works | Run full Bronze → Silver → Gold |

### Phase 3: Setup Other Platforms (Tuần 4)

| # | Task | Description |
|---|------|-------------|
| 3.1 | Tạo Facebook platform skeleton | `platforms/facebook/` structure |
| 3.2 | Tạo TikTok platform skeleton | `platforms/tiktok/` structure |
| 3.3 | Tạo 1Offices platform skeleton | `platforms/oneoffices/` structure |

### Phase 4: Setup Registry & Pipelines (Tuần 5)

| # | Task | Description |
|---|------|-------------|
| 4.1 | Implement FeatureRegistry | `registry/feature_registry.py` |
| 4.2 | Implement auto-discovery | Tự động load features |
| 4.3 | Create BronzePipeline | `pipelines/bronze_pipeline.py` |
| 4.4 | Create SilverPipeline | `pipelines/silver_pipeline.py` |
| 4.5 | Create GoldPipeline | `pipelines/gold_pipeline.py` |
| 4.6 | Update main.py | Use registry and pipelines |
| 4.7 | Remove old orchestrator | Clean up `orchestrator.py` |

### Phase 5: Cleanup & Documentation (Tuần 6)

| # | Task | Description |
|---|------|-------------|
| 5.1 | Remove old structure | Delete `extractors/`, `transformers/`, `loaders/` |
| 5.2 | Update README.md | New structure documentation |
| 5.3 | Update EXTENSIBILITY_GUIDE.md | How to add new features |
| 5.4 | Integration testing | Full pipeline test |

---

## 🔀 Migration Strategy

### Backward Compatibility

Để đảm bảo không break production, áp dụng **Strangler Fig Pattern**:

1. **Parallel Run**: Giữ cấu trúc cũ hoạt động song song
2. **Feature Flag**: Dùng config để switch giữa old/new implementation
3. **Gradual Migration**: Migrate từng feature một
4. **Verification**: So sánh output giữa old/new before switching

```python
# src/config.py
FEATURE_FLAGS = {
    'use_new_bills_extractor': False,  # Set True sau khi verify
    'use_new_products_extractor': False,
    # ...
}
```

---

## ✅ Benefits sau Refactoring

| Aspect | Before | After |
|--------|--------|-------|
| **Add new entity** | Sửa 4-5 files ở nhiều folders | Chỉ tạo 1 folder mới với template |
| **Find code** | Search nhiều folders | Mọi thứ trong `platforms/{platform}/features/{entity}/` |
| **Testing** | Tests tách rời từ code | Co-located tests |
| **Code ownership** | Khó assign | 1 team/person per feature |
| **Deployment** | All-or-nothing | Feature-level deployment possible |
| **Dependencies** | Implicit | Explicit qua interfaces |
---

## 📝 Checklist

- [ ] Phase 1: Core & Shared setup
- [ ] Phase 2: Nhanh platform migration
- [ ] Phase 3: Other platforms skeleton
- [ ] Phase 4: Registry & Orchestration
- [ ] Phase 5: Cleanup & Documentation
- [ ] Final verification

---

## 🚀 Next Steps

1. Review và approve kế hoạch này
2. Estimate effort cho từng phase
3. Assign team members
4. Start implementation

---

*Tài liệu được tạo: 2025-12-06*

