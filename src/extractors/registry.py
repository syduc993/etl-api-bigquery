"""
Extractor Registry - Quản lý và đăng ký các extractors.
File này cung cấp registry pattern để dễ dàng thêm extractors mới
cho các endpoints và platforms khác nhau.
"""
from typing import Dict, Type, Optional, List
from abc import ABC, abstractmethod
from src.utils.logging import get_logger

logger = get_logger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class cho tất cả extractors.
    
    Class này định nghĩa interface chung cho extractors từ
    các platforms khác nhau (Nhanh, Facebook, TikTok, 1Offices...).
    """
    
    def __init__(self, platform: str, entity: str):
        """
        Khởi tạo base extractor.
        
        Args:
            platform: Tên platform (ví dụ: 'nhanh', 'facebook', 'tiktok')
            entity: Tên entity (ví dụ: 'bills', 'products', 'orders')
        """
        self.platform = platform
        self.entity = entity
    
    @abstractmethod
    def extract(self, **kwargs) -> List[Dict]:
        """
        Extract data từ API.
        
        Args:
            **kwargs: Các tham số cụ thể cho từng extractor
            
        Returns:
            List[Dict]: Danh sách records đã extract
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict:
        """
        Lấy schema của entity này.
        
        Returns:
            Dict: Schema definition
        """
        pass
    
    def get_metadata(self) -> Dict:
        """
        Lấy metadata của extractor.
        
        Returns:
            Dict: Metadata (platform, entity, version, etc.)
        """
        return {
            "platform": self.platform,
            "entity": self.entity,
            "extractor_class": self.__class__.__name__
        }


class ExtractorRegistry:
    """
    Registry để quản lý và đăng ký extractors.
    
    Registry này cho phép:
    - Đăng ký extractors từ nhiều platforms
    - Tìm extractors theo platform và entity
    - List tất cả extractors
    - Dễ dàng mở rộng với extractors mới
    """
    
    def __init__(self):
        """Khởi tạo registry."""
        self._extractors: Dict[str, Dict[str, Type[BaseExtractor]]] = {}
        # Structure: {platform: {entity: ExtractorClass}}
    
    def register(
        self,
        platform: str,
        entity: str,
        extractor_class: Type[BaseExtractor]
    ):
        """
        Đăng ký một extractor.
        
        Args:
            platform: Tên platform (ví dụ: 'nhanh', 'facebook')
            entity: Tên entity (ví dụ: 'bills', 'orders')
            extractor_class: Class của extractor (phải kế thừa BaseExtractor)
        """
        if platform not in self._extractors:
            self._extractors[platform] = {}
        
        self._extractors[platform][entity] = extractor_class
        
        logger.info(
            f"Registered extractor",
            platform=platform,
            entity=entity,
            class_name=extractor_class.__name__
        )
    
    def get(
        self,
        platform: str,
        entity: str
    ) -> Optional[Type[BaseExtractor]]:
        """
        Lấy extractor class theo platform và entity.
        
        Args:
            platform: Tên platform
            entity: Tên entity
            
        Returns:
            Optional[Type[BaseExtractor]]: Extractor class hoặc None nếu không tìm thấy
        """
        return self._extractors.get(platform, {}).get(entity)
    
    def list_platforms(self) -> List[str]:
        """
        List tất cả platforms đã đăng ký.
        
        Returns:
            List[str]: Danh sách platforms
        """
        return list(self._extractors.keys())
    
    def list_entities(self, platform: str) -> List[str]:
        """
        List tất cả entities của một platform.
        
        Args:
            platform: Tên platform
            
        Returns:
            List[str]: Danh sách entities
        """
        return list(self._extractors.get(platform, {}).keys())
    
    def list_all(self) -> Dict[str, List[str]]:
        """
        List tất cả extractors đã đăng ký.
        
        Returns:
            Dict[str, List[str]]: {platform: [entities]}
        """
        return {
            platform: list(entities.keys())
            for platform, entities in self._extractors.items()
        }
    
    def create_instance(
        self,
        platform: str,
        entity: str,
        **kwargs
    ) -> Optional[BaseExtractor]:
        """
        Tạo instance của extractor.
        
        Args:
            platform: Tên platform
            entity: Tên entity
            **kwargs: Arguments để pass vào extractor constructor
            
        Returns:
            Optional[BaseExtractor]: Extractor instance hoặc None
        """
        extractor_class = self.get(platform, entity)
        if extractor_class:
            return extractor_class(**kwargs)
        return None


# Global registry instance
registry = ExtractorRegistry()

