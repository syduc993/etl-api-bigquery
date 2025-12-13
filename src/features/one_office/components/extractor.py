"""
Extractor for 1Office Personnel Data.
Responsible for fetching data from 1Office API.
"""
import requests
import time
from typing import List, Dict, Any, Optional
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class OneOfficeExtractor:
    """
    Component chuyên trách việc lấy dữ liệu từ 1Office API.
    """
    
    def __init__(self):
        self.base_url = settings.oneoffice_base_url
        self.access_token = settings.oneoffice_access_token
        
    def fetch_all_personnel(self) -> List[Dict[str, Any]]:
        """
        Lấy toàn bộ danh sách hồ sơ nhân sự.
        Tự động xử lý phân trang (pagination).
        
        Returns:
            List[Dict]: Danh sách các hồ sơ nhân sự.
        """
        logger.info("Starting to fetch all personnel profiles from 1Office...")
        
        all_data = []
        page = 1
        limit = 100
        
        if not self.access_token:
            logger.warning("No access token provided for 1Office API. Please set ONEOFFICE_ACCESS_TOKEN.")
            # Có thể raise error hoặc return empty tùy strategy. 
            # Ở đây return empty để tránh crash nếu config chưa xong.
            raise ValueError("Missing ONEOFFICE_ACCESS_TOKEN")

        while True:
            try:
                # Construct URL and params
                url = f"{self.base_url}/personnel/profile/gets"
                params = {
                    "access_token": self.access_token,
                    "limit": limit,
                    "page": page
                }
                
                # Call API
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Check response structure
                # 1Office response thường có dạng: {"data": [...], "error": ..., "message": ...}
                # Cần verify key chứa list data. Giả định là 'data'.
                if not isinstance(data, dict):
                     logger.error(f"Unexpected response format (not dict): {data}")
                     break

                items = data.get('data', [])
                
                if not items:
                    logger.info(f"Page {page}: No more data. Loop finished.")
                    break
                    
                count = len(items)
                logger.info(f"Page {page}: Fetched {count} items.")
                
                all_data.extend(items)
                page += 1
                
                # Rate limit prevention (simple sleep)
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed at page {page}: {e}")
                raise e
            except Exception as e:
                logger.error(f"Unexpected error at page {page}: {e}")
                raise e
                
        logger.info(f"Finished fetching. Total personnel records: {len(all_data)}")
        return all_data
