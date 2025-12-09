"""
Structured logging configuration cho ETL pipeline.
Sử dụng module này thay vì logging trực tiếp để có structured logs
tương thích với Cloud Logging.
"""
import logging
import json
from datetime import datetime
from typing import Any, Dict
from src.config import settings


class StructuredLogger:
    """
    Structured logger cho ETL pipeline.
    Hỗ trợ log với structured data, tương thích với Google Cloud Logging format.
    """
    
    def __init__(self, name: str):
        """
        Khởi tạo structured logger.
        
        Args:
            name: Tên logger (thường là __name__)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, settings.log_level))
        
        self.logger.handlers = []
        
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, settings.log_level))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def _log(self, level: str, message: str, extra: Dict[str, Any] = None):
        """Internal logging method với structured data."""
        log_message = f"{message}"
        if extra:
            log_message += f" | {json.dumps(extra)}"
        
        getattr(self.logger, level.lower())(log_message)
    
    def info(self, message: str, **kwargs):
        """Log info message với optional structured data."""
        self._log("INFO", message, kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message với optional structured data."""
        self._log("WARNING", message, kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message với optional structured data."""
        self._log("ERROR", message, kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message với optional structured data."""
        self._log("DEBUG", message, kwargs)


def get_logger(name: str) -> StructuredLogger:
    """
    Lấy một structured logger instance.
    
    Args:
        name: Tên logger (thường là __name__)
        
    Returns:
        StructuredLogger: Logger instance
    """
    return StructuredLogger(name)
