"""
Custom exceptions cho ETL pipeline.
File này định nghĩa các exception classes được sử dụng trong pipeline.
"""


class NhanhAPIError(Exception):
    """
    Base exception cho các lỗi từ Nhanh API.
    
    Tất cả các lỗi liên quan đến Nhanh API đều kế thừa từ exception này.
    """
    pass


class RateLimitError(NhanhAPIError):
    """
    Exception được raise khi vượt quá API rate limit.
    
    Attributes:
        locked_seconds: Số giây bị khóa
        unlocked_at: Timestamp khi được unlock
    """
    
    def __init__(self, message: str, locked_seconds: int = None, unlocked_at: int = None):
        """
        Khởi tạo RateLimitError.
        
        Args:
            message: Error message
            locked_seconds: Số giây bị khóa
            unlocked_at: Timestamp khi được unlock
        """
        super().__init__(message)
        self.locked_seconds = locked_seconds
        self.unlocked_at = unlocked_at


class AuthenticationError(NhanhAPIError):
    """
    Exception được raise khi authentication với API thất bại.
    
    Thường xảy ra khi:
    - Access token không hợp lệ
    - App ID hoặc Business ID không đúng
    - Token đã hết hạn
    """
    pass


class PaginationError(NhanhAPIError):
    """
    Exception được raise khi xử lý pagination thất bại.
    
    Thường xảy ra khi:
    - Format của paginator.next không đúng
    - Không thể lấy được trang tiếp theo
    """
    pass


class DataValidationError(Exception):
    """
    Exception được raise khi data validation thất bại.
    
    Thường xảy ra trong Silver layer khi:
    - Schema validation fail
    - Data type conversion fail
    - Business rule validation fail
    """
    pass


class WatermarkError(Exception):
    """
    Exception được raise khi watermark tracking thất bại.
    
    Thường xảy ra khi:
    - Không thể đọc watermark từ BigQuery
    - Không thể update watermark
    """
    pass
