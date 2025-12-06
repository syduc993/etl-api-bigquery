"""
Unit tests cho ExtractorRegistry module.
File này test Registry pattern để quản lý extractors.
"""
import pytest
from unittest.mock import Mock, patch


class TestBaseExtractor:
    """Test suite cho BaseExtractor abstract class."""
    
    def test_base_extractor_is_abstract(self):
        """Test BaseExtractor không thể instantiate trực tiếp."""
        from src.extractors.registry import BaseExtractor
        
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            BaseExtractor(platform='test', entity='test')
    
    def test_concrete_extractor_get_metadata(self):
        """Test get_metadata trả về đúng format."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        # Create a concrete implementation for testing
        class TestExtractor(BaseExtractor):
            def extract(self, **kwargs) -> List[Dict]:
                return []
            
            def get_schema(self) -> Dict:
                return {'entity': 'test', 'fields': []}
        
        extractor = TestExtractor(platform='test_platform', entity='test_entity')
        metadata = extractor.get_metadata()
        
        assert metadata['platform'] == 'test_platform'
        assert metadata['entity'] == 'test_entity'
        assert metadata['extractor_class'] == 'TestExtractor'


class TestExtractorRegistry:
    """Test suite cho ExtractorRegistry."""
    
    def setup_method(self):
        """Setup cho mỗi test - tạo fresh registry."""
        from src.extractors.registry import ExtractorRegistry
        self.registry = ExtractorRegistry()
    
    def test_register_and_get(self):
        """Test đăng ký và lấy extractor."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        class MockExtractor(BaseExtractor):
            def extract(self, **kwargs) -> List[Dict]:
                return []
            def get_schema(self) -> Dict:
                return {}
        
        self.registry.register('test_platform', 'test_entity', MockExtractor)
        
        result = self.registry.get('test_platform', 'test_entity')
        
        assert result == MockExtractor
    
    def test_get_not_found(self):
        """Test get trả về None khi không tìm thấy."""
        result = self.registry.get('nonexistent', 'entity')
        
        assert result is None
    
    def test_list_platforms(self):
        """Test list_platforms trả về danh sách platforms."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        class MockExtractor(BaseExtractor):
            def extract(self, **kwargs) -> List[Dict]:
                return []
            def get_schema(self) -> Dict:
                return {}
        
        self.registry.register('platform_a', 'entity1', MockExtractor)
        self.registry.register('platform_b', 'entity1', MockExtractor)
        
        platforms = self.registry.list_platforms()
        
        assert 'platform_a' in platforms
        assert 'platform_b' in platforms
    
    def test_list_entities(self):
        """Test list_entities trả về danh sách entities của platform."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        class MockExtractor(BaseExtractor):
            def extract(self, **kwargs) -> List[Dict]:
                return []
            def get_schema(self) -> Dict:
                return {}
        
        self.registry.register('platform_a', 'entity1', MockExtractor)
        self.registry.register('platform_a', 'entity2', MockExtractor)
        
        entities = self.registry.list_entities('platform_a')
        
        assert 'entity1' in entities
        assert 'entity2' in entities
    
    def test_list_entities_empty(self):
        """Test list_entities trả về empty list khi platform không tồn tại."""
        entities = self.registry.list_entities('nonexistent')
        
        assert entities == []
    
    def test_list_all(self):
        """Test list_all trả về tất cả extractors."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        class MockExtractor(BaseExtractor):
            def extract(self, **kwargs) -> List[Dict]:
                return []
            def get_schema(self) -> Dict:
                return {}
        
        self.registry.register('platform_a', 'entity1', MockExtractor)
        self.registry.register('platform_a', 'entity2', MockExtractor)
        self.registry.register('platform_b', 'entity3', MockExtractor)
        
        all_extractors = self.registry.list_all()
        
        assert 'platform_a' in all_extractors
        assert 'platform_b' in all_extractors
        assert 'entity1' in all_extractors['platform_a']
        assert 'entity2' in all_extractors['platform_a']
        assert 'entity3' in all_extractors['platform_b']
    
    def test_create_instance(self):
        """Test create_instance tạo instance của extractor."""
        from src.extractors.registry import BaseExtractor
        from typing import Dict, List
        
        class MockExtractor(BaseExtractor):
            def __init__(self, **kwargs):
                super().__init__(platform='mock', entity='mock')
            
            def extract(self, **kwargs) -> List[Dict]:
                return []
            
            def get_schema(self) -> Dict:
                return {}
        
        self.registry.register('mock', 'mock', MockExtractor)
        
        instance = self.registry.create_instance('mock', 'mock')
        
        assert instance is not None
        assert isinstance(instance, MockExtractor)
    
    def test_create_instance_not_found(self):
        """Test create_instance trả về None khi không tìm thấy."""
        instance = self.registry.create_instance('nonexistent', 'entity')
        
        assert instance is None


class TestGlobalRegistry:
    """Test suite cho global registry instance."""
    
    def test_global_registry_exists(self):
        """Test global registry instance exists."""
        from src.extractors.registry import registry
        from src.extractors.registry import ExtractorRegistry
        
        assert registry is not None
        assert isinstance(registry, ExtractorRegistry)
    
    def test_nhanh_extractors_registered(self):
        """Test Nhanh extractors đã được đăng ký."""
        from src.extractors import registry
        
        platforms = registry.list_platforms()
        
        # Nhanh platform should be registered
        assert 'nhanh' in platforms
        
        # Check some expected entities
        entities = registry.list_entities('nhanh')
        assert 'bills' in entities
