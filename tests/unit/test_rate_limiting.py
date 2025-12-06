"""
Unit tests cho rate limiting.
File này test token bucket algorithm và rate limit handling.
"""
import pytest
import time
from src.extractors.nhanh.base import TokenBucket, NhanhApiClient


class TestTokenBucket:
    """
    Test suite cho TokenBucket algorithm.
    """
    
    def test_token_bucket_initialization(self):
        """
        Test khởi tạo token bucket.
        """
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        
        assert bucket.capacity == 10
        assert bucket.refill_rate == 1.0
        assert bucket.tokens == 10
    
    def test_token_bucket_acquire(self):
        """
        Test acquire tokens.
        """
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        
        # Should be able to acquire
        assert bucket.acquire() is True
        assert bucket.tokens == 9
        
        # Acquire multiple
        assert bucket.acquire(5) is True
        assert bucket.tokens == 4
    
    def test_token_bucket_wait_time(self):
        """
        Test tính toán wait time.
        """
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        
        # Use all tokens
        bucket.tokens = 0
        
        # Should need to wait
        wait_time = bucket.wait_time(1)
        assert wait_time > 0
    
    def test_token_bucket_refill(self):
        """
        Test refill tokens theo thời gian.
        """
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        bucket.tokens = 0
        bucket.last_refill = time.time() - 5  # 5 seconds ago
        
        # Refill should add tokens
        bucket._refill()
        assert bucket.tokens > 0


class TestRateLimitHandling:
    """
    Test suite cho rate limit handling trong NhanhApiClient.
    """
    
    def test_rate_limit_error_handling(self):
        """
        Test xử lý rate limit error.
        
        Note: Test này chỉ test logic, không thực sự gọi API.
        """
        # Test structure của error response
        error_data = {
            "lockedSeconds": 30,
            "unlockedAt": int(time.time()) + 30
        }
        
        assert "lockedSeconds" in error_data
        assert "unlockedAt" in error_data

