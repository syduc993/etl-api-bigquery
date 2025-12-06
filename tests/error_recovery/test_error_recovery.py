"""
Error recovery tests.
File này test khả năng recovery sau khi có lỗi xảy ra.
"""
import pytest
from datetime import datetime
from src.loaders.watermark import WatermarkTracker
from src.extractors.nhanh.base import NhanhApiClient
from src.utils.exceptions import RateLimitError, AuthenticationError


class TestErrorRecovery:
    """
    Test suite cho error recovery.
    
    Tests bao gồm:
    - Recovery sau rate limit
    - Recovery sau network errors
    - Recovery sau partial failures
    - Watermark recovery
    """
    
    @pytest.fixture
    def watermark_tracker(self):
        """
        Fixture tạo WatermarkTracker instance.
        
        Returns:
            WatermarkTracker: Tracker instance
        """
        return WatermarkTracker()
    
    def test_watermark_recovery(self, watermark_tracker):
        """
        Test recovery sử dụng watermark.
        
        Args:
            watermark_tracker: WatermarkTracker fixture
        """
        # Simulate failed extraction
        entity = "test_recovery"
        
        # Update watermark với timestamp cũ
        old_time = datetime.utcnow()
        watermark_tracker.update_watermark(entity, old_time, 100)
        
        # Get incremental range - should start from old_time
        from_date, to_date = watermark_tracker.get_incremental_range(entity)
        
        assert from_date is not None
        # from_date should be close to old_time
        time_diff = abs((from_date - old_time).total_seconds())
        assert time_diff < 60  # Within 1 minute
    
    def test_partial_failure_recovery(self):
        """
        Test recovery sau partial failure.
        
        Scenario: Extract 1000 bills, upload 500 thành công, 500 fail.
        Should be able to resume từ watermark.
        """
        # This is a conceptual test
        # In practice, watermark should track last successful extraction
        watermark_tracker = WatermarkTracker()
        
        # Simulate partial success
        watermark_tracker.update_watermark(
            "bills",
            datetime.utcnow(),
            records_count=500
        )
        
        # Next run should continue from watermark
        from_date, to_date = watermark_tracker.get_incremental_range("bills")
        assert from_date is not None
        assert to_date is not None

