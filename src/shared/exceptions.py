"""
Custom exceptions cho ETL pipeline.
"""


class NhanhAPIError(Exception):
    """Base exception cho các lỗi từ Nhanh API."""
    pass


class RateLimitError(NhanhAPIError):
    """
    Exception được raise khi vượt quá API rate limit.
    
    Attributes:
        locked_seconds: Số giây bị khóa
        unlocked_at: Timestamp khi được unlock
    """
    
    def __init__(self, message: str, locked_seconds: int = None, unlocked_at: int = None):
        super().__init__(message)
        self.locked_seconds = locked_seconds
        self.unlocked_at = unlocked_at


class AuthenticationError(NhanhAPIError):
    """Exception được raise khi authentication với API thất bại."""
    pass


class PaginationError(NhanhAPIError):
    """Exception được raise khi xử lý pagination thất bại."""
    pass


class DataValidationError(Exception):
    """Exception được raise khi data validation thất bại."""
    pass


class WatermarkError(Exception):
    """Exception được raise khi watermark tracking thất bại."""
    pass
