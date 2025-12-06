"""
Unit tests cho extractors module.
File này test các extractors (BillExtractor, ProductExtractor, NhanhApiClient)
với mocked API responses.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta


class TestTokenBucket:
    """Test suite cho TokenBucket algorithm."""
    
    def test_initialization(self):
        """Test khởi tạo TokenBucket với capacity và refill_rate."""
        from src.extractors.nhanh.base import TokenBucket
        
        bucket = TokenBucket(capacity=150, refill_rate=5.0)
        
        assert bucket.capacity == 150
        assert bucket.refill_rate == 5.0
        assert bucket.tokens == 150
    
    def test_acquire_success(self):
        """Test acquire tokens thành công."""
        from src.extractors.nhanh.base import TokenBucket
        
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        
        assert bucket.acquire(5) is True
        assert bucket.tokens == 5
    
    def test_acquire_not_enough_tokens(self):
        """Test acquire khi không đủ tokens."""
        from src.extractors.nhanh.base import TokenBucket
        
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        bucket.tokens = 2
        
        assert bucket.acquire(5) is False
        assert bucket.tokens == 2  # Không thay đổi
    
    def test_wait_time_calculation(self):
        """Test tính toán wait time."""
        from src.extractors.nhanh.base import TokenBucket
        
        bucket = TokenBucket(capacity=10, refill_rate=2.0)
        bucket.tokens = 0
        
        # Cần 4 tokens, refill 2 tokens/giây => cần 2 giây
        wait_time = bucket.wait_time(4)
        assert wait_time >= 2.0


class TestNhanhApiClient:
    """Test suite cho NhanhApiClient."""
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_split_date_range_no_split(self, mock_creds):
        """Test split_date_range khi không cần chia (range <= 31 ngày)."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.base import NhanhApiClient
        
        client = NhanhApiClient()
        from_date = datetime(2024, 1, 1)
        to_date = datetime(2024, 1, 20)  # 20 ngày
        
        chunks = client.split_date_range(from_date, to_date)
        
        assert len(chunks) == 1
        assert chunks[0] == (from_date, to_date)
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_split_date_range_needs_split(self, mock_creds):
        """Test split_date_range khi cần chia (range > 31 ngày)."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.base import NhanhApiClient
        
        client = NhanhApiClient()
        from_date = datetime(2024, 1, 1)
        to_date = datetime(2024, 3, 15)  # ~74 ngày
        
        chunks = client.split_date_range(from_date, to_date)
        
        # Phải có nhiều hơn 1 chunk
        assert len(chunks) >= 2
        
        # Mỗi chunk không nên quá 31 ngày
        for chunk_from, chunk_to in chunks:
            delta = (chunk_to - chunk_from).days
            assert delta <= 31
        
        # Chunk đầu bắt đầu từ from_date
        assert chunks[0][0] == from_date
        
        # Chunk cuối kết thúc tại to_date
        assert chunks[-1][1] == to_date


class TestBillExtractor:
    """Test suite cho BillExtractor."""
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_schema(self, mock_creds):
        """Test get_schema trả về đúng format."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.bill import BillExtractor
        
        extractor = BillExtractor()
        schema = extractor.get_schema()
        
        assert schema['entity'] == 'bills'
        assert schema['platform'] == 'nhanh'
        assert 'fields' in schema
        assert len(schema['fields']) > 0
        
        # Check required fields exist
        field_names = [f['name'] for f in schema['fields']]
        assert 'id' in field_names
        assert 'date' in field_names
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_metadata(self, mock_creds):
        """Test get_metadata trả về thông tin đúng."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.bill import BillExtractor
        
        extractor = BillExtractor()
        metadata = extractor.get_metadata()
        
        assert metadata['platform'] == 'nhanh'
        assert metadata['entity'] == 'bills'
        assert metadata['extractor_class'] == 'BillExtractor'
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    @patch.object(__import__('src.extractors.nhanh.bill', fromlist=['BillExtractor']).BillExtractor, 'fetch_paginated')
    def test_extract_calls_fetch_bills(self, mock_fetch, mock_creds):
        """Test extract() gọi fetch_bills và trả về data."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        mock_fetch.return_value = [
            {'id': 1, 'date': '2024-01-15'},
            {'id': 2, 'date': '2024-01-16'}
        ]
        
        from src.extractors.nhanh.bill import BillExtractor
        
        extractor = BillExtractor()
        
        # Mock fetch_paginated để không gọi API thật
        extractor.fetch_paginated = mock_fetch
        
        result = extractor.extract()
        
        assert isinstance(result, list)


class TestProductExtractor:
    """Test suite cho ProductExtractor."""
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_schema(self, mock_creds):
        """Test get_schema trả về đúng format."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.product import ProductExtractor
        
        extractor = ProductExtractor()
        schema = extractor.get_schema()
        
        assert schema['entity'] == 'products'
        assert schema['platform'] == 'nhanh'
        assert 'fields' in schema
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_metadata(self, mock_creds):
        """Test get_metadata trả về thông tin đúng."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.product import ProductExtractor
        
        extractor = ProductExtractor()
        metadata = extractor.get_metadata()
        
        assert metadata['platform'] == 'nhanh'
        assert metadata['entity'] == 'products'
        assert metadata['extractor_class'] == 'ProductExtractor'


class TestCustomerExtractor:
    """Test suite cho CustomerExtractor."""
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_schema(self, mock_creds):
        """Test get_schema trả về đúng format."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.customer import CustomerExtractor
        
        extractor = CustomerExtractor()
        schema = extractor.get_schema()
        
        assert schema['entity'] == 'customers'
        assert schema['platform'] == 'nhanh'
        assert 'fields' in schema
    
    @patch('src.extractors.nhanh.base.get_nhanh_credentials')
    def test_get_metadata(self, mock_creds):
        """Test get_metadata trả về thông tin đúng."""
        mock_creds.return_value = {
            'app_id': 'test_app',
            'business_id': 'test_business',
            'access_token': 'test_token'
        }
        
        from src.extractors.nhanh.customer import CustomerExtractor
        
        extractor = CustomerExtractor()
        metadata = extractor.get_metadata()
        
        assert metadata['platform'] == 'nhanh'
        assert metadata['entity'] == 'customers'
        assert metadata['extractor_class'] == 'CustomerExtractor'
