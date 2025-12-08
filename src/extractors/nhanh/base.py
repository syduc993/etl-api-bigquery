"""
Base client cho Nhanh API.
File này cung cấp lớp cơ sở với các tính năng:
- Authentication với appId, businessId, accessToken
- Rate limiting sử dụng token bucket algorithm (150 req/30s)
- Pagination handling (hỗ trợ next là object/array)
- Error handling và retry logic
- Date range splitting cho 31-day limit
"""
import time
import random
from typing import Dict, Any, Optional, List
import requests
from datetime import datetime, timedelta
from src.config import settings, get_nhanh_credentials
from src.utils.exceptions import (
    NhanhAPIError,
    RateLimitError,
    AuthenticationError,
    PaginationError
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """
    Token bucket algorithm để implement rate limiting.
    
    Thuật toán này đảm bảo không vượt quá rate limit của API
    bằng cách chỉ cho phép request khi có đủ tokens.
    """
    
    def __init__(self, capacity: int, refill_rate: float):
        """
        Khởi tạo token bucket.
        
        Args:
            capacity: Số lượng tokens tối đa (ví dụ: 150)
            refill_rate: Số tokens được thêm vào mỗi giây (ví dụ: 5 tokens/giây)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
    
    def acquire(self, tokens: int = 1) -> bool:
        """
        Thử lấy tokens để thực hiện request.
        
        Args:
            tokens: Số lượng tokens cần lấy (mặc định: 1)
            
        Returns:
            True nếu có đủ tokens và đã lấy thành công, False nếu không đủ
        """
        self._refill()
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self):
        """
        Refill tokens dựa trên thời gian đã trôi qua.
        
        Tính toán số tokens cần thêm dựa trên refill_rate
        và thời gian từ lần refill cuối cùng.
        """
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.refill_rate
        
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def wait_time(self, tokens: int = 1) -> float:
        """
        Tính toán thời gian cần đợi để có đủ tokens.
        
        Args:
            tokens: Số lượng tokens cần (mặc định: 1)
            
        Returns:
            float: Thời gian đợi (giây). 0.0 nếu đã có đủ tokens
        """
        self._refill()
        
        if self.tokens >= tokens:
            return 0.0
        
        needed = tokens - self.tokens
        return needed / self.refill_rate


class NhanhApiClient:
    """
    Base client cho Nhanh API.
    
    Client này cung cấp các tính năng:
    - Authentication tự động
    - Rate limiting với token bucket
    - Pagination handling
    - Error handling và retry
    - Date range splitting
    """
    
    def __init__(self):
        """
        Khởi tạo Nhanh API client.
        
        Tự động load credentials từ Secret Manager và setup
        token bucket cho rate limiting.
        """
        self.base_url = settings.nhanh_api_base_url
        self.credentials = get_nhanh_credentials()
        
        # Token bucket for rate limiting (150 requests per 30 seconds)
        self.token_bucket = TokenBucket(
            capacity=settings.nhanh_rate_limit,
            refill_rate=settings.nhanh_rate_limit / settings.nhanh_rate_window
        )
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Authorization": self.credentials["accessToken"]
        })
    
    def _wait_for_rate_limit(self):
        """
        Đợi nếu rate limit đã đạt.
        
        Tính toán thời gian cần đợi dựa trên token bucket
        và thêm jitter để tránh thundering herd problem.
        """
        wait_time = self.token_bucket.wait_time()
        if wait_time > 0:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.5)
            sleep_time = wait_time + jitter
            logger.warning(
                f"Rate limit reached, waiting {sleep_time:.2f} seconds",
                wait_time=sleep_time
            )
            time.sleep(sleep_time)
    
    def _handle_rate_limit_error(self, error_data: Dict[str, Any]) -> None:
        """
        Xử lý lỗi ERR_429 rate limit.
        
        Hàm này parse lockedSeconds và unlockedAt từ error response
        và đợi cho đến khi được unlock.
        
        Args:
            error_data: Error data từ API response (chứa lockedSeconds, unlockedAt)
            
        Note:
            Nếu có unlockedAt thì đợi đến thời điểm đó,
            nếu không thì đợi lockedSeconds giây.
        """
        locked_seconds = error_data.get("lockedSeconds", 30)
        unlocked_at = error_data.get("unlockedAt")
        
        if unlocked_at:
            # Calculate wait time until unlock
            current_time = int(time.time())
            wait_seconds = max(0, unlocked_at - current_time)
            
            logger.warning(
                f"Rate limited. Locked for {locked_seconds}s, unlocking at {unlocked_at}",
                locked_seconds=locked_seconds,
                unlocked_at=unlocked_at,
                wait_seconds=wait_seconds
            )
            
            if wait_seconds > 0:
                time.sleep(wait_seconds)
        else:
            # Fallback: wait for locked_seconds
            logger.warning(
                f"Rate limited. Waiting {locked_seconds} seconds",
                locked_seconds=locked_seconds
            )
            time.sleep(locked_seconds)
    
    def _make_request(
        self,
        endpoint: str,
        body: Dict[str, Any],
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> Dict[str, Any]:
        """
        Thực hiện API request với retry logic.
        
        Hàm này tự động:
        - Kiểm tra và đợi rate limit
        - Xử lý ERR_429 rate limit errors
        - Retry với exponential backoff cho transient errors
        - Validate response code (code: 1 = success, code: 0 = failed)
        
        Args:
            endpoint: API endpoint (ví dụ: '/bill/list')
            body: Request body (JSON)
            max_retries: Số lần retry tối đa (mặc định: 3)
            retry_delay: Delay ban đầu giữa các retries (giây)
            
        Returns:
            Dict[str, Any]: API response dưới dạng dictionary
            
        Raises:
            NhanhAPIError: Nếu request thất bại sau tất cả retries
            AuthenticationError: Nếu authentication thất bại
        """
        # Construct URL with version
        url = f"{self.base_url}/v3.0{endpoint}"
        params = {
            "appId": self.credentials["appId"],
            "businessId": self.credentials["businessId"]
        }
        
        for attempt in range(max_retries + 1):
            try:
                # Wait for rate limit
                self._wait_for_rate_limit()
                
                # Acquire token
                if not self.token_bucket.acquire():
                    self._wait_for_rate_limit()
                    self.token_bucket.acquire()
                
                # Make request
                response = self.session.post(url, params=params, json=body, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                
                # Check response code
                if result.get("code") == 0:
                    error_code = result.get("errorCode", "UNKNOWN_ERROR")
                    messages = result.get("messages", "Unknown error")
                    
                    # Log full error response for debugging
                    logger.error(
                        f"API error response",
                        error_code=error_code,
                        messages=messages,
                        full_response=result
                    )
                    
                    # Handle rate limit error
                    if error_code == "ERR_429":
                        error_data = result.get("data", {})
                        self._handle_rate_limit_error(error_data)
                        # Retry after handling rate limit
                        continue
                    
                    # Handle authentication errors
                    if error_code in ["ERR_INVALID_ACCESS_TOKEN", "ERR_INVALID_APP_ID", "ERR_INVALID_BUSINESS_ID"]:
                        raise AuthenticationError(f"Authentication failed: {error_code}")
                    
                    # Other errors
                    raise NhanhAPIError(f"API error {error_code}: {messages}")
                
                # Success
                return result
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries + 1}), retrying in {delay:.2f}s",
                        error=str(e),
                        endpoint=endpoint
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"Request failed after {max_retries + 1} attempts", error=str(e))
                    raise NhanhAPIError(f"Request failed: {str(e)}")
        
        raise NhanhAPIError("Request failed after all retries")
    
    def fetch_paginated(
        self,
        endpoint: str,
        body: Dict[str, Any],
        data_key: str = "data"
    ) -> List[Dict[str, Any]]:
        """
        Lấy tất cả các pages của data sử dụng pagination.
        
        Hàm này tự động loop qua tất cả các pages cho đến khi
        không còn next page. Hỗ trợ paginator.next là object/array.
        
        Args:
            endpoint: API endpoint
            body: Initial request body (sẽ được modify cho pagination)
            data_key: Key trong response chứa data array (mặc định: "data")
            
        Returns:
            List[Dict[str, Any]]: Danh sách tất cả records từ tất cả pages
            
        Raises:
            PaginationError: Nếu pagination thất bại
        """
        all_data = []
        paginator = body.get("paginator", {})
        page_num = 1
        
        logger.info(f"Starting paginated fetch for {endpoint}", endpoint=endpoint)
        
        while True:
            try:
                # Make request
                response = self._make_request(endpoint, body)
                
                # Extract data
                page_data = response.get(data_key, [])
                if not page_data:
                    logger.warning(f"No data in page {page_num}", page=page_num)
                    break
                
                all_data.extend(page_data)
                logger.info(
                    f"Fetched page {page_num}: {len(page_data)} records",
                    page=page_num,
                    records=len(page_data),
                    total=len(all_data)
                )
                
                # Check for next page
                paginator_response = response.get("paginator", {})
                next_page = paginator_response.get("next")
                
                if not next_page:
                    logger.info(f"Reached end of pagination at page {page_num}", total_records=len(all_data))
                    break
                
                # Update paginator for next request
                # Note: next can be object/array, not string
                if isinstance(next_page, (dict, list)):
                    body["paginator"]["next"] = next_page
                else:
                    # Fallback: if it's a string, try to parse it
                    logger.warning(f"Unexpected next format: {type(next_page)}", next=next_page)
                    body["paginator"]["next"] = next_page
                
                page_num += 1
                
            except Exception as e:
                logger.error(
                    f"Error fetching page {page_num}",
                    error=str(e),
                    page=page_num,
                    total_fetched=len(all_data)
                )
                raise PaginationError(f"Pagination failed at page {page_num}: {str(e)}")
        
        logger.info(f"Completed paginated fetch: {len(all_data)} total records", total_records=len(all_data))
        return all_data
    
    def split_date_range(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> List[tuple]:
        """
        Chia date range thành các chunks theo max_date_range_days.
        
        Hàm này cần thiết vì Nhanh API chỉ hỗ trợ query tối đa 31 ngày.
        Nếu date range lớn hơn, sẽ tự động chia thành các chunks.
        
        Args:
            from_date: Ngày bắt đầu
            to_date: Ngày kết thúc
            
        Returns:
            List[tuple]: Danh sách các (start_date, end_date) tuples
            
        Example:
            Nếu from_date = 2024-01-01, to_date = 2024-03-01 (60 ngày)
            Sẽ trả về 2 chunks: [(2024-01-01, 2024-01-31), (2024-02-01, 2024-03-01)]
        """
        max_days = settings.nhanh_max_date_range_days
        chunks = []
        
        current_start = from_date
        
        while current_start < to_date:
            current_end = min(
                current_start + timedelta(days=max_days - 1),
                to_date
            )
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)
        
        return chunks
    
    def split_date_range_by_day(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> List[tuple]:
        """
        Chia date range thành từng ngày riêng biệt.
        
        Hàm này chia date range thành các chunks 1 ngày, mỗi ngày là một chunk.
        Hữu ích khi muốn xử lý và lưu data theo từng ngày riêng biệt.
        
        Args:
            from_date: Ngày bắt đầu
            to_date: Ngày kết thúc
            
        Returns:
            List[tuple]: Danh sách các (start_date, end_date) tuples, mỗi tuple là 1 ngày
            
        Example:
            Nếu from_date = 2024-12-01, to_date = 2024-12-10
            Sẽ trả về 10 chunks: [(2024-12-01, 2024-12-01), (2024-12-02, 2024-12-02), ...]
        """
        chunks = []
        current_date = from_date.date()
        end_date = to_date.date()
        
        while current_date <= end_date:
            # Tạo datetime cho start và end của ngày (00:00:00 đến 23:59:59)
            day_start = datetime.combine(current_date, datetime.min.time())
            day_end = datetime.combine(current_date, datetime.max.time().replace(microsecond=0))
            chunks.append((day_start, day_end))
            current_date += timedelta(days=1)
        
        return chunks

